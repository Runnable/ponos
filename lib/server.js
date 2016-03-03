'use strict'

const clone = require('101/clone')
const defaults = require('101/defaults')
const ErrorCat = require('error-cat')
const hermes = require('runnable-hermes')
const isFunction = require('101/is-function')
const isObject = require('101/is-object')
const isString = require('101/is-string')
const pick = require('101/pick')
const Promise = require('bluebird')

const logger = require('./logger')
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
  constructor (opts) {
    this._tasks = {}
    this._workerOptions = {}
    this.opts = Object.assign({}, opts)

    this.log = this.opts.log || logger.child({ module: 'ponos:server' })
    this.errorCat = this.opts.errorCat || new ErrorCat()

    if (this.opts.hermes) {
      this.hermes = this.opts.hermes
    } else {
      if (!Array.isArray(this.opts.queues)) {
        throw new Error('ponos.server: missing required `queues` option.')
      }
      if (!this.opts.queues.every(isString)) {
        throw new Error(
          'ponos.server: each element of `queues` must be a string.'
        )
      }
      // Defaults for the hermes client's rabbitmq connection
      defaults(this.opts, {
        hostname: process.env.RABBITMQ_HOSTNAME || 'localhost',
        port: process.env.RABBITMQ_PORT || 5672,
        username: process.env.RABBITMQ_USERNAME || 'guest',
        password: process.env.RABBITMQ_PASSWORD || 'guest',
        name: 'ponos'
      })

      this.hermes = hermes.hermesSingletonFactory(this.opts)
    }

    this.hermes = Promise.promisifyAll(this.hermes)
  }

  /**
   * Helper function that subscribes to a given queue name.
   * @private
   * @param {string} queueName Name of the queue.
   */
  _subscribe (queueName) {
    this.log.trace('subscribing to ' + queueName)
    if (!this._tasks[queueName]) {
      this.log.warn({ queueName: queueName }, 'handler not defined')
      return
    }
    this.hermes.subscribe(queueName, (job, done) => {
      this._runWorker(queueName, job, done)
    })
  }

  /**
   * Helper function to subscribe to all queues.
   * @private
   * @return {promise} Resolved when queues are all subscribed.
   */
  _subscribeAll () {
    this.log.trace('_subscribeAll')
    return Promise.resolve(this.hermes.getQueues()).bind(this)
      .map(this._subscribe)
  }

  /**
   * Helper function that unsubscribes to a given queue name.
   * @private
   * @param {string} queueName Name of the queue.
   * @return {promise} A promise that resolves post worker creation
   *   and subscription.
   */
  _unsubscribe (queueName) {
    this.log.trace('unsubscribing from ' + queueName)
    const handler = null // null will remove all handlers
    return this.hermes.unsubscribeAsync(queueName, handler)
  }

  /**
   * Helper function to unsubscribe from all queues.
   * @private
   * @return {promise} Resolved when queues are all subscribed.
   */
  _unsubscribeAll () {
    this.log.trace('_unsubscribeAll')
    return Promise.resolve(this.hermes.getQueues()).bind(this)
      .map(this._unsubscribe)
  }

  /**
   * Runs a worker for the given queue name, job, and acknowledgement callback.
   * @private
   * @param {string} queueName Name of the queue.
   * @param {object} job Job for the worker to perform.
   * @param {function} done RabbitMQ acknowledgement callback.
   */
  _runWorker (queueName, job, done) {
    this.log.trace({ queue: queueName, job: job }, '_runWorker')
    let opts = clone(this._workerOptions[queueName])
    defaults(opts, {
      queue: queueName,
      job: job,
      task: this._tasks[queueName],
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
    return this.hermes.connectAsync().bind(this)
      .then(this._subscribeAll)
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
    return this._unsubscribeAll().bind(this)
      .then(() => {
        return this.hermes.closeAsync()
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
  setAllTasks (map) {
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

  /**
   * Assigns a task to a queue.
   * @param {string} queueName Queue name.
   * @param {function} task Function to take a job and return a promise.
   * @param {object} opts Options for the worker that performs the task.
   * @returns {ponos.Server} The server.
   */
  setTask (queueName, task, opts) {
    this.log.trace({ queue: queueName }, 'setting task for queue')
    if (!isFunction(task)) {
      throw new Error('ponos.server: setTask task handler must be a function')
    }
    this._tasks[queueName] = task
    this._workerOptions[queueName] = isObject(opts)
      ? pick(opts, 'msTimeout')
      : {}
    return this
  }
}

module.exports = Server
