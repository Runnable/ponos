/* @flow */
/* global Bluebird$Promise Logger */
'use strict'
const assign = require('101/assign')
const clone = require('101/clone')
const defaults = require('101/defaults')
const ErrorCat = require('error-cat')
const Immutable = require('immutable')
const isFunction = require('101/is-function')
const isObject = require('101/is-object')
const Promise = require('bluebird')
const put = require('101/put')

const logger = require('./logger')
const RabbitMQ = require('./rabbitmq')
const RedisRateLimiter = require('./rate-limiters/redis')
const Worker = require('./worker')

/**
 * Ponos server class. Given a queue adapter the worker server will
 * connect to RabbitMQ, subscribe to the given queues, and begin spawning
 * workers for incoming jobs.
 *
 * The only required option is `opts.queues` which should be a non-empty flat
 * list of strings. The server uses this list to subscribe to only queues you
 * have provided.
 *
 * @author Bryan Kendall
 * @author Ryan Sandor Richards
 * @param {Object} opts Options for the server.
 * @param {ErrorCat} [opts.errorCat] An error cat instance to use for the
 *   server.
 * @param {Object<String, Function>} [opts.events] Mapping of event (fanout)
 *   exchanges which to subscribe and handlers.
 * @param {bunyan} [opts.log] A bunyan logger to use for the server.
 * @param {String} [opts.name=ponos] A name to namespace the created exchange queues.
 * @param {Object} [opts.rabbitmq] RabbitMQ connection options.
 * @param {Object} [opts.rabbitmq.channel] RabbitMQ channel options.
 * @param {Object} [opts.rabbitmq.channel.prefetch] Set prefetch for each
 *   consumer in a channel.
 * @param {String} [opts.rabbitmq.hostname=localhost] Hostname for RabbitMQ. Can
 *   be set with environment variable RABBITMQ_HOSTNAME.
 * @param {Number} [opts.rabbitmq.port=5672] Port for RabbitMQ. Can be set with
 *   environment variable RABBITMQ_PORT.
 * @param {String} [opts.rabbitmq.username] Username for RabbitMQ. Can be set
 *   with environment variable RABBITMQ_USERNAME.
 * @param {String} [opts.rabbitmq.password] Username for Password. Can be set
 *   with environment variable RABBITMQ_PASSWORD.
 * @param {Object<String, Function>} [opts.tasks] Mapping of queues to subscribe
 *   directly with handlers.
 * @param {Object} [opts.redisRateLimiter] options for redis-rate-limiter. checkout
 *   module for params
 */
class Server {
  _events: Map<string, Function>;
  _opts: Object;
  _rabbitmq: RabbitMQ;
  _redisRateLimiter: RedisRateLimiter;
  _tasks: Map<string, Function>;
  _workerOptions: Object;
  _workQueues: Object;

  errorCat: ErrorCat;
  log: Logger;

  constructor (opts: Object) {
    this._opts = assign({}, opts)
    this.log = this._opts.log || logger.child({ module: 'ponos:server' })
    this._workerOptions = {}
    this._workQueues = {}

    this._tasks = new Immutable.Map()
    if (this._opts.tasks) {
      this.setAllTasks(this._opts.tasks)
    }
    this._events = new Immutable.Map()
    if (this._opts.events) {
      this.setAllEvents(this._opts.events)
    }

    this.errorCat = this._opts.errorCat || ErrorCat

    if (this._opts.redisRateLimiter) {
      this._redisRateLimiter = new RedisRateLimiter(put(this._opts.redisRateLimiter, {
        log: this.log
      }))
    }

    // add the name to RabbitMQ options
    const rabbitmqOpts = defaults(
      this._opts.rabbitmq || {},
      { name: this._opts.name }
    )
    this._rabbitmq = new RabbitMQ(rabbitmqOpts)
  }

  /**
   * Start consuming from the subscribed queues. This is called by `.start`.
   * This can be called after the server has been started to start consuming
   * from additional queues.
   *
   * @return {Promise} Promise resolved when consuming has started.
   */
  consume (): Bluebird$Promise<void> {
    return this._rabbitmq.consume().return()
  }

  /**
   * Starts the worker server, connects to RabbitMQ, subscribes and consumes
   * from all the provided queues and exchanges (tasks and events).
   *
   * @return {Promise} Promise that resolves once the server is listening.
   */
  start (): Bluebird$Promise<void> {
    this.log.trace('starting')
    return this._rabbitmq.connect()
      .then(() => {
        if (this._redisRateLimiter) {
          return this._redisRateLimiter.connect()
        }
      })
      .then(() => {
        return this._subscribeAll()
      })
      .then(() => {
        return this.consume()
      })
      .then(() => {
        this.log.trace('started')
      })
      .catch((err) => {
        this.errorCat.report(err)
        throw err
      })
  }

  /**
   * Stops the worker server, unsubscribing and disconnecting from RabbitMQ.
   *
   * @return {Promise} A promise that resolves when the server is stopped.
   */
  stop (): Bluebird$Promise<void> {
    this.log.trace('stopping')
    return this._rabbitmq.unsubscribe()
      .then(() => {
        return this._rabbitmq.disconnect()
      })
      .then(() => {
        this.log.trace('stopped')
      })
      .catch((err) => {
        this.errorCat.report(err)
        throw err
      })
  }

  /**
   * Takes a map of queues and task handlers and sets them all.
   *
   * @param {Object<String, Function>} map A map of queue names and task
   *   handlers.
   * @param {String} map.key Queue name.
   * @param {Object} map.value Object with a handler and additional options for
   *   the worker (must have a `.task` handler function)
   * @param {Function} map.value Handler function to take a job.
   * @returns {Server} The server.
   */
  setAllTasks (map: Object): Server {
    if (!isObject(map)) {
      throw new Error('ponos.server: setAllTasks must be called with an object')
    }
    Object.keys(map).forEach((key) => {
      this._workQueues[key] = []
      const value = map[key]
      if (isObject(value)) {
        if (!isFunction(value.task)) {
          this.log.warn({ key: key }, 'no task function defined for key')
          return
        }
        this.setTask(key, value.task, value)
      } else {
        this.setTask(key, map[key])
      }
    })
    return this
  }

  /**
   * Takes a map of event exchanges and handlers and subscribes to them all.
   *
   * @param {Object<String, Function>} map A map of exchanges and task handlers.
   * @param {String} map.key Exchange name.
   * @param {Object} map.value Object with handler and additional options for
   *   the worker (must have a `.task` handler function)
   * @param {Function} map.value Handler function to take a job.
   * @returns {Server} The server.
   */
  setAllEvents (map: Object): Server {
    if (!isObject(map)) {
      throw new Error('ponos.server: setAllEvents must be called with an object')
    }
    Object.keys(map).forEach((key) => {
      this._workQueues[key] = []
      const value = map[key]
      if (isObject(value)) {
        if (!isFunction(value.task)) {
          this.log.warn({ key: key }, 'no task function defined for key')
          return
        }
        this.setEvent(key, value.task, value)
      } else {
        this.setEvent(key, map[key])
      }
    })
    return this
  }

  /**
   * Assigns a task to a queue.
   *
   * @param {String} queueName Queue name.
   * @param {Function} task Function to take a job and return a promise.
   * @param {Object} [opts] Options for the worker that performs the task.
   * @returns {Server} The server.
   */
  setTask (queueName: string, task: Function, opts?: Object): Server {
    this.log.trace({
      queue: queueName,
      method: 'setTask'
    }, 'setting task for queue')
    if (!isFunction(task)) {
      throw new Error('ponos.server: setTask task handler must be a function')
    }
    if (opts && !isObject(opts)) {
      throw new Error('ponos.server: setTask opts must be a object')
    }

    this._tasks = this._tasks.set(queueName, task)
    this._workerOptions[queueName] = opts || {}
    return this
  }

  /**
   * Assigns a task to an exchange.
   *
   * @param {String} exchangeName Exchange name.
   * @param {Function} task Function to take a job and return a promise.
   * @param {Object} [opts] Options for the worker that performs the task.
   * @returns {Server} The server.
   */
  setEvent (exchangeName: string, task: Function, opts?: Object): Server {
    this.log.trace({
      exchange: exchangeName,
      method: 'setEvent'
    }, 'setting task for queue')
    if (!isFunction(task)) {
      throw new Error('ponos.server: setEvent task handler must be a function')
    }
    if (opts && !isObject(opts)) {
      throw new Error('ponos.server: setEvent opts must be a object')
    }

    this._events = this._events.set(exchangeName, task)
    this._workerOptions[exchangeName] = opts || {}
    return this
  }

  // Private Methods

  /**
   * Helper function to subscribe to all queues.
   *
   * @private
   * @return {Promise} Promise that resolves when queues are all subscribed.
   */
  _subscribeAll (): Bluebird$Promise<void> {
    this.log.trace('_subscribeAll')
    const tasks = this._tasks
    const events = this._events
    return Promise.map(tasks.keySeq(), (queue) => {
      return this._rabbitmq.subscribeToQueue(
        queue,
        (job, jobMeta, done) => {
          this._enqueue(queue, tasks.get(queue), job, jobMeta, done)
        }
      )
    })
    .then(() => {
      return Promise.map(events.keySeq(), (exchange) => {
        return this._rabbitmq.subscribeToFanoutExchange(
          exchange,
          (job, jobMeta, done) => {
            this._enqueue(exchange, events.get(exchange), job, jobMeta, done)
          }
        )
      })
    })
    .return()
  }

  /**
   * Adds worker to queue and starts work loop if there is work to do
   * @param  {String}   name    name of queue
   * @param  {Promise}  worker  worker promise to run
   * @param  {Object}   job     job for worker
   * @param  {Object}   jobMeta job metadata
   * @param  {Function} done    worker callback
   * @return {undefined}
   */
  _enqueue (name: string, worker: Promise<*>, job: Object, jobMeta: Object, done: Function) {
    this._workQueues[name].push(this._runWorker.bind(this, name, worker, job, jobMeta, done))
    // we are already processing _workQueues
    if (this._workQueues[name].length === 1) {
      // this is first job in _workQueues, start the loop
      this._workLoop(name)
    }
  }

  /**
   * Loop which pops items off the run queue and executes them
   * this runs asynchronously to caller
   * @param  {String} name  name of task or event
   * @return {Promise}
   * @resolves {undefined}
   */
  _workLoop (name: string) {
    return Promise.try(() => {
      if (this._redisRateLimiter) {
        return this._redisRateLimiter.limit(name, this._workerOptions[name])
      }
    })
    .catch((err) => {
      // ignore rate limiter errors, just continue
      this.errorCat.report(err)
    })
    .then(() => {
      const worker = this._workQueues[name].pop()
      if (worker) {
        // run worker and start next task in parallel
        worker()
        this._workLoop(name)
      }
    })
  }

  /**
   * Runs a worker for the given queue name, job, and acknowledgement callback.
   *
   * @private
   * @param {String} queueName Name of the queue.
   * @param {Function} handler Handler to perform the work.
   * @param {Object} job Job for the worker to perform.
   * @param {Object} jobMeta Job's metadata with appId, timestamp and additonal headers.
   * @param {Function} done RabbitMQ acknowledgement callback.
   * @return {Promise}
   * @resolves {undefined} when worker is successful
   * @rejects {Error} when creating Worker fails
   */
  _runWorker (
    queueName: string,
    handler: Promise<*>,
    job: Object,
    jobMeta: Object,
    done: Function
  ): Promise<*> {
    return Promise.try(() => {
      this.log.info({
        queue: queueName,
        job: job,
        jobMeta: jobMeta,
        method: '_runWorker'
      }, 'running worker')
      const opts = clone(this._workerOptions[queueName])
      defaults(opts, {
        queue: queueName,
        job: job,
        jobMeta: jobMeta,
        task: handler,
        log: this.log,
        errorCat: this.errorCat
      })
      const worker = Worker.create(opts)
      return worker.run().finally(() => {
        // done has to be called with no arguments
        done()
      })
    })
  }
}

/**
 * Server class.
 * @module ponos/lib/server
 * @see Server
 */
module.exports = Server
