'use strict'

const defaults = require('101/defaults')
const ErrorCat = require('error-cat')
const exists = require('101/exists')
const isNumber = require('101/is-number')
const isObject = require('101/is-object')
const merge = require('101/put')
const monitor = require('monitor-dog')
const pick = require('101/pick')
const Promise = require('bluebird')

const TimeoutError = Promise.TimeoutError

const logger = require('./logger')
const TaskFatalError = require('./errors/task-fatal-error')

/**
 * Worker class: performs tasks for jobs on a given queue.
 * @class
 * @author Bryan Kendall
 * @author Ryan Sandor Richards
 * @module ponos:worker
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
class Worker {
  constructor (opts) {
    // managed required fields
    const fields = [
      'done',
      'job',
      'queue',
      'task'
    ]
    fields.forEach(function (f) {
      if (!exists(opts[f])) {
        throw new Error(f + ' is required for a Worker')
      }
    })

    // manage field defaults
    fields.push('errorCat', 'log', 'msTimeout', 'runNow')
    opts = pick(opts, fields)
    defaults(opts, {
      // default non-required user options
      errorCat: new ErrorCat(),
      log: logger.child({ module: 'ponos:worker' }),
      runNow: true,
      // other options
      attempt: 0,
      msTimeout: process.env.WORKER_TIMEOUT || 0,
      retryDelay: process.env.WORKER_MIN_RETRY_DELAY || 1
    })

    // put all opts on this
    Object.assign(this, opts)
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
  static create (opts) {
    return new Worker(opts)
  }

  /**
   * Helper function for reporting errors to rollbar via error-cat.
   * @private
   * @param {error} err Error to report.
   */
  _reportError (err) {
    err.data = isObject(err.data) ? err.data : {}
    err.data.queue = this.queue
    err.data.job = this.job
    this.errorCat.report(err)
  }

  /**
   * Helper function for creating monitor-dog events tags
   * `queue` is the only mandatory tag.
   * Few tags would be created depending on the queue name
   * If queueName use `.` as delimiter e.x. `10.0.0.20.api.github.push` then
   * following tags would be created:
   * - token0: 'push'
   * - token1: 'github.push'
   * - token2: 'api.github.push'
   * - token3: '10.0.0.20.api.github.push'
   * @private
   * @returns {Object} tags as Object {queue: 'docker.event.publish'}
   */
  _eventTags () {
    const tokens = this.queue.split('.').reverse()
    let lastToken = ''
    let tags = tokens.reduce(function (acc, currentValue, currentIndex) {
      const key = 'token' + currentIndex
      const newToken = currentIndex === 0
        ? currentValue
        : currentValue + '.' + lastToken
      acc[key] = newToken
      lastToken = newToken
      return acc
    }, {})
    tags.queue = this.queue
    return tags
  }

  /**
   * Helper function calling `monitor.increment`.
   * Monitor wouldn't be called if `process.env.WORKER_MONITOR_DISABLED` set.
   * @param {string} eventName name to be reported into the datadog
   * @param {object} [extraTags] extra tags to be send with the event
   * @private
   */
  _incMonitor (eventName, extraTags) {
    if (process.env.WORKER_MONITOR_DISABLED) {
      return
    }
    let tags = this._eventTags()
    if (extraTags) {
      tags = merge(tags, extraTags)
    }
    monitor.increment(eventName, tags)
  }

  /**
   * Helper function calling `monitor.timer`.
   * Timer wouldn't be created if `process.env.WORKER_MONITOR_DISABLED` set
   * @return {object} new timer
   * @private
   */
  _createTimer () {
    const tags = this._eventTags()
    return !process.env.WORKER_MONITOR_DISABLED
      ? monitor.timer('ponos.timer', true, tags)
      : null
  }

  /**
   * Runs the worker. If the task for the job fails, then this method will retry
   * the task (with an exponential backoff) a number of times defined by the
   * environment of the process.
   * @returns {promise} Promise resolved once the task succeeds or fails.
   */
  run () {
    const log = this.log.child({
      method: 'run',
      queue: this.queue,
      job: this.job
    })
    this._incMonitor('ponos')
    const timer = this._createTimer()
    return Promise.resolve().bind(this)
      .then(function runTheTask () {
        const attemptData = {
          attempt: this.attempt++,
          timeout: this.msTimeout
        }
        log.info(attemptData, 'running task')
        let taskPromise = Promise.resolve().bind(this)
          .then(() => {
            return Promise.resolve().bind(this)
              .then(() => { return this.task(this.job) })
          })
        if (this.msTimeout) {
          taskPromise = taskPromise.timeout(this.msTimeout)
        }
        return taskPromise
      })
      .then(function successDone (result) {
        log.info({ result: result }, 'Task complete')
        this._incMonitor('ponos.finish', { result: 'success' })
        return this.done()
      })
      // if the type is TimeoutError, we will log and retry
      .catch(TimeoutError, function timeoutErrRetry (err) {
        log.warn({ err: err }, 'Task timed out')
        this._incMonitor('ponos.finish', { result: 'timeout-error' })
        // by throwing this type of error, we will retry :)
        throw err
      })
      // if it's a known type of error, we can't accomplish the task
      .catch(TaskFatalError, function knownErrDone (err) {
        log.error({ err: err }, 'Worker task fatally errored')
        this._incMonitor('ponos.finish', { result: 'fatal-error' })
        this._reportError(err)
        // If we encounter a fatal error we should no longer try to schedule
        // the job.
        return this.done()
      })
      .catch(function unknownErrRetry (err) {
        const attemptData = {
          err: err,
          nextAttemptDelay: this.retryDelay
        }
        log.warn(attemptData, 'Task failed, retrying')
        this._incMonitor('ponos.finish', { result: 'task-error' })
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
      .finally(function stopTimer () {
        if (timer) {
          timer.stop()
        }
      })
  }
}

module.exports = Worker
