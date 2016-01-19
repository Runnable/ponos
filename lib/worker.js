'use strict'

var Promise = require('bluebird')
var TaskFatalError = require('./errors/task-fatal-error')
var TimeoutError = Promise.TimeoutError
var assign = require('101/assign')
var defaults = require('101/defaults')
var ErrorCat = require('error-cat')
var monitor = require('monitor-dog')
var exists = require('101/exists')
var isNumber = require('101/is-number')
var isObject = require('101/is-object')
var log = require('./logger')
var merge = require('101/put')
var pick = require('101/pick')

/**
 * Worker module, for performing job tasks.
 * @author Bryan Kendall
 * @author Ryan Sandor Richards
 * @module ponos:worker
 */
module.exports = Worker

/**
 * Worker class: performs tasks for jobs on a given queue.
 * @class
 * @param {object} opts Options for the worker.
 * @param {string} opts.queue Name of the queue for the job the worker
 *   is processing.
 * @param {function} opts.task A function to handle the tasks.
 * @param {object} opts.job Data for the job to process.
 * @param {function} opts.done Callback to execute when the job has successfully
 *   been completed.
 * @param {boolean} [opts.runNow] Whether or not to run the job immediately,
 *   defaults to `true`.
 * @param {bunyan} [opts.log] The bunyan logger to use when logging messages
 *   from the worker.
 * @param {ErrorCat} [opts.errorCat] An error-cat instance to use for the
 *   worker.
 * @param {number} [opts.msTimeout] A specific millisecond timeout for this
 *   worker.
 */
function Worker (opts) {
  // managed required fields
  var fields = [
    'queue',
    'task',
    'job',
    'done'
  ]
  fields.forEach(function (f) {
    if (!exists(opts[f])) {
      throw new Error(f + ' is required for a Worker')
    }
  })

  // manage field defaults
  fields.push('runNow', 'msTimeout', 'log', 'errorCat')
  opts = pick(opts, fields)
  defaults(opts, {
    // default non-required user options
    runNow: true,
    log: log,
    errorCat: new ErrorCat(),
    // other options
    attempt: 0,
    msTimeout: process.env.WORKER_TIMEOUT || 0,
    retryDelay: process.env.WORKER_MIN_RETRY_DELAY || 1
  })

  // put all opts on this
  assign(this, opts)
  this.log.info({ queue: this.queue, job: this.job }, 'Worker created')

  // Ensure that the `msTimeout` option is valid
  this.msTimeout = parseInt(this.msTimeout, 10)
  if (!isNumber(this.msTimeout)) {
    throw new Error('Provided `msTimeout` is not an integer')
  }

  if (this.msTimeout < 0) {
    throw new Error('Provided `msTimeout` is negative')
  }

  if (this.runNow) {
    this.run()
  }
}

/**
 * Factory method for creating new workers. This method exists to make it easier
 * to unit test other modules that need to instantiate new workers.
 * @see Worker
 * @param {object} opts Worker options.
 * @returns {Worker} New Worker.
 */
Worker.create = function (opts) {
  return new Worker(opts)
}

/**
 * Helper function for reporting errors to rollbar via error-cat.
 * @private
 * @param {error} err Error to report.
 */
Worker.prototype._reportError = function (err) {
  err.data = isObject(err.data) ? err.data : {}
  err.data.queue = this.queue
  err.data.job = this.job
  this.errorCat.report(err)
}

/**
 * Helper function for creating monitor-dog events tags
 * @private
 * @param {Object} tags as Object {queue: 'docker.event.publish'}
 */
Worker.prototype._eventTags = function () {
  var tokens = this.queue.split('.').reverse()
  var lastToken = ''
  var tags = tokens.reduce(function (acc, currentValue, currentIndex) {
    var key = 'token' + currentIndex
    var newToken = (currentIndex === 0 ? currentValue : currentValue + '.' + lastToken)
    acc[key] = newToken
    lastToken = newToken
    return acc
  }, {})
  tags.queue = this.queue
  return tags
}

Worker.prototype._inc = function (eventName, extraTags) {
  if (process.env.WORKER_MONITOR_DISABLED) {
    return
  }
  var tags = this._eventTags()
  if (extraTags) {
    tags = merge(tags, extraTags)
  }
  monitor.increment(eventName, tags)
}

/**
 * Runs the worker. If the task for the job fails, then this method will retry
 * the task (with an exponential backoff) a number of times defined by the
 * environment of the process.
 * @returns {promise} Promise resolved once the task succeeds or fails.
 */
Worker.prototype.run = function () {
  var jobAndQueueData = {
    queue: this.queue,
    job: this.job
  }
  this._inc('ponos')
  var timer = monitor.timer('ponos.timer', this._eventTags())
  return Promise.resolve().bind(this)
    .then(function runTheTask () {
      var attemptData = {
        attempt: this.attempt++,
        timeout: this.msTimeout
      }
      this.log.info(merge(jobAndQueueData, attemptData), 'Running task')
      var taskPromise = Promise.resolve().bind(this)
        .then(function () {
          return Promise.resolve().bind(this)
            .then(function () { return this.task(this.job) })
        })
      if (this.msTimeout) {
        taskPromise = taskPromise.timeout(this.msTimeout)
      }
      return taskPromise
    })
    .then(function successDone (result) {
      this.log.info(
        merge(jobAndQueueData, { result: result }),
        'Task complete'
      )
      this._inc('ponos.finish', { result: 'success' })
      return this.done()
    })
    // if the type is TimeoutError, we will log and retry
    .catch(TimeoutError, function timeoutErrRetry (err) {
      log.warn(merge(jobAndQueueData, { err: err }), 'Task timed out')
      this._inc('ponos.finish', { result: 'timeout-error' })
      // just by throwing this type of error, we will retry :)
      throw err
    })
    // if it's a known type of error, we can't accomplish the task
    .catch(TaskFatalError, function knownErrDone (err) {
      this.log.error({ err: err }, 'Worker task fatally errored')
      this._inc('ponos.finish', { result: 'fatal-error' })
      this._reportError(err)
      // If we encounter a fatal error we should no longer try to schedule
      // the job.
      return this.done()
    })
    .catch(function unknownErrRetry (err) {
      var attemptData = {
        err: err,
        nextAttemptDelay: this.retryDelay
      }
      this.log.warn(
        merge(jobAndQueueData, attemptData),
        'Task failed, retrying'
      )
      this._inc('ponos.finish', { result: 'task-error' })
      this._reportError(err)

      // Try again after a delay
      return Promise.delay(this.retryDelay).bind(this)
        .then(function retryRun () {
          // Exponentially increase the retry delay
          if (this.retryDelay < process.env.WORKER_MAX_RETRY_DELAY) {
            this.retryDelay *= 2
          }
          return this.run()
        })
    })
    .finally(function () {
      if (process.env.WORKER_MONITOR_DISABLED) {
        return
      }
      timer.stop()
    })
}
