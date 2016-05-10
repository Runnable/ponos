/* @flow */
/* global ErrorCat */
'use strict'

const clone = require('101/clone')
const defaults = require('101/defaults')
const errorCat = require('error-cat')
const Immutable = require('immutable')
const isFunction = require('101/is-function')
const isObject = require('101/is-object')
const pick = require('101/pick')
const Promise = require('bluebird')

const logger = require('./logger')
const RabbitMQ = require('./rabbitmq')
const Worker = require('./worker')

/**
 * Ponos worker server class. Given a queue adapter the worker server will
 * connect to RabbitMQ, subscribe to the given queues, and begin spawning
 * workers for incoming jobs.
 *
 * The only required option is `opts.queues` which should be a non-empty flat
 * list of strings. The server uses this list to subscribe to only queues you
 * have provided.
 *
 * @class
 * @module ponos:server
 * @author Bryan Kendall
 * @author Ryan Sandor Richards
 * @param {object} opts Options for the server.
 * @param {Array} opts.queues An array of queue names to which the server should
 *   subscribe.
 * @param {runnable-hermes~Hermes} [opts.hermes] A hermes client.
 * @param {string} [opts.hostname] Hostname for RabbitMQ.
 * @param {string|number} [opts.port] Port for RabbitMQ.
 * @param {string} [opts.username] Username for RabbitMQ.
 * @param {string} [opts.password] Username for Password.
 * @param {bunyan} [opts.log] A bunyan logger to use for the server.
 * @param {ErrorCat} [opts.errorCat] An error cat instance to use for the
 *   server.
 */
class Server {
  _events: Map<string, Function>;
  _rabbitmq: any;
  _tasks: Map<string, Function>;
  _workerOptions: Object;
  errorCat: ErrorCat;
  log: Object;
  opts: Object;

  constructor (opts: Object) {
    this.opts = Object.assign({}, opts)
    this.log = this.opts.log || logger.child({ module: 'ponos:server' })
    this._workerOptions = {}

    this._tasks = new Immutable.Map()
    if (this.opts.tasks) {
      this.setAllTasks(this.opts.tasks)
    }
    this._events = new Immutable.Map()
    if (this.opts.events) {
      this.setAllEvents(this.opts.events)
    }

    this.errorCat = this.opts.errorCat || errorCat

    this._rabbitmq = new RabbitMQ()
  }

  consume () {
    return this._rabbitmq.consume()
  }

  /**
   * Helper function to subscribe to all queues.
   * @private
   * @return {promise} Resolved when queues are all subscribed.
   */
  _subscribeAll () {
    this.log.trace('_subscribeAll')
    const tasks = this._tasks
    const events = this._events
    return Promise.map(tasks.keySeq(), (queue) => {
      return this._rabbitmq.subscribeToQueue(queue, (job, done) => {
        this._runWorker(queue, tasks.get(queue), job, done)
      })
    })
      .then(() => {
        return Promise.map(events.keySeq(), (exchange) => {
          return this._rabbitmq.subscribeToFanoutExchange(
            exchange,
            (job, done) => {
              this._runWorker(exchange, events.get(exchange), job, done)
            }
          )
        })
      })
  }

  /**
   * Runs a worker for the given queue name, job, and acknowledgement callback.
   * @private
   * @param {string} queueName Name of the queue.
   * @param {function} handler Handler to perform the work.
   * @param {object} job Job for the worker to perform.
   * @param {function} done RabbitMQ acknowledgement callback.
   */
  _runWorker (
    queueName: string,
    handler: Function,
    job: Object,
    done: Function
  ) {
    this.log.trace({
      queue: queueName,
      job: job,
      method: '_runWorker'
    }, 'running worker')
    const opts = clone(this._workerOptions[queueName])
    defaults(opts, {
      queue: queueName,
      job: job,
      task: handler,
      done: done,
      log: this.log,
      errorCat: this.errorCat
    })
    Worker.create(opts)
  }

  /**
   * Starts the worker server and listens for jobs coming from all queues.
   * @return {promise} A promise that resolves when the server is listening.
   */
  start () {
    this.log.trace('starting')
    return this._rabbitmq.connect()
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
   * Stops the worker server.
   * @return {promise} A promise that resolves when the server is stopped.
   */
  stop () {
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
   * @param {object} map A map of queue names to task handlers.
   * @param {string} map.key Queue name.
   * @param {function} map.value Function to take a job or an object with a task
   *   and additional options for the worker.
   * @returns {ponos.Server} The server.
   */
  setAllTasks (map: Object) {
    if (!isObject(map)) {
      throw new Error('ponos.server: setAllTasks must be called with an object')
    }
    Object.keys(map).forEach((key) => {
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

  setAllEvents (map: Object) {
    if (!isObject(map)) {
      throw new Error('ponos.server: setAllEvents must be called with an object')
    }
    Object.keys(map).forEach((key) => {
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
   * @param {string} queueName Queue name.
   * @param {function} task Function to take a job and return a promise.
   * @param {object} opts Options for the worker that performs the task.
   * @returns {ponos.Server} The server.
   */
  setTask (queueName: string, task: Function, opts?: Object) {
    this.log.trace({
      queue: queueName,
      method: 'setTask'
    }, 'setting task for queue')
    if (!isFunction(task)) {
      throw new Error('ponos.server: setTask task handler must be a function')
    }
    this._tasks = this._tasks.set(queueName, task)
    this._workerOptions[queueName] = opts && isObject(opts)
      ? pick(opts, 'msTimeout')
      : {}
    return this
  }

  setEvent (exchangeName: string, task: Function, opts?: Object) {
    this.log.trace({
      exchange: exchangeName,
      method: 'setEvent'
    }, 'setting task for queue')
    if (!isFunction(task)) {
      throw new Error('ponos.server: setEvent task handler must be a function')
    }
    this._events = this._events.set(exchangeName, task)
    this._workerOptions[exchangeName] = opts && isObject(opts)
      ? pick(opts, 'msTimeout')
      : {}
    return this
  }
}

module.exports = Server
