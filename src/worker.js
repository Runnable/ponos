/* @flow */
/* global Logger WorkerError DDTimer */
'use strict'

const cls = require('continuation-local-storage').createNamespace('ponos')
const clsBlueBird = require('cls-bluebird')
const defaults = require('101/defaults')
const ErrorCat = require('error-cat')
const isObject = require('101/is-object')
const joi = require('joi')
const merge = require('101/put')
const monitor = require('monitor-dog')
const Promise = require('bluebird')
const uuid = require('uuid')
const WorkerStopError = require('error-cat/errors/worker-stop-error')

const TimeoutError = Promise.TimeoutError
clsBlueBird(cls)

const optsSchema = joi.object({
  done: joi.func().required(),
  job: joi.object().required(),
  log: joi.object().required(),
  queue: joi.string().required(),
  task: joi.func().required(),
  errorCat: joi.object(),
  finalRetryFn: joi.func(),
  maxNumRetries: joi.number(),
  msTimeout: joi.number().positive(),
  runNow: joi.bool()
})

/**
 * Performs tasks for jobs on a given queue.
 *
 * @author Bryan Kendall
 * @author Ryan Sandor Richards
 * @param {Object} opts Options for the worker.
 * @param {Function} opts.done Callback to execute when the job has successfully
 *   been completed.
 * @param {Object} opts.job Data for the job to process.
 * @param {String} opts.queue Name of the queue for the job the worker is
 *   processing.
 * @param {Function} opts.task A function to handle the tasks.
 * @param {ErrorCat} [opts.errorCat] An error-cat instance to use for the
 *   worker.
 * @param {bunyan} [opts.log] The bunyan logger to use when logging messages
 *   from the worker.
 * @param {number} [opts.msTimeout] A specific millisecond timeout for this
 *   worker.
 * @param {boolean} [opts.runNow] Whether or not to run the job immediately,
 *   defaults to `true`.
 */
class Worker {
  attempt: number;
  done: Function;
  errorCat: ErrorCat;
  finalRetryFn: Function;
  job: Object;
  log: Logger;
  maxNumRetries: number;
  msTimeout: number;
  queue: String;
  retryDelay: number;
  task: Function;
  tid: String;

  constructor (opts: Object) {
    // managed required fields
    joi.assert(opts, optsSchema)
    opts = joi.validate(opts, optsSchema, { stripUnknown: true }).value

    defaults(opts, {
      // default non-required user options
      errorCat: ErrorCat,
      runNow: true,
      // other options
      attempt: 0,
      finalRetryFn: Promise.resolve(),
      maxNumRetries: process.env.WORKER_MAX_NUM_RETRIES || 0,
      msTimeout: process.env.WORKER_TIMEOUT || 0,
      retryDelay: process.env.WORKER_MIN_RETRY_DELAY || 1
    })

    this.tid = opts.job.tid || uuid()
    opts.log = opts.log.child({ tid: this.tid, module: 'ponos:worker' })
    // put all opts on this
    Object.assign(this, opts)
    this.log.info({ queue: this.queue, job: this.job }, 'Worker created')

    if (this.runNow) {
      this.run()
    }
  }

  /**
   * Factory method for creating new workers. This method exists to make it
   * easier to unit test other modules that need to instantiate new workers.
   *
   * @see Worker
   * @param {Object} opts Options for the Worker.
   * @returns {Worker} New Worker.
   */
  static create (opts: Object): Worker {
    return new Worker(opts)
  }

  /**
   * Runs the worker. If the task for the job fails, then this method will retry
   * the task (with an exponential backoff) as set by the environment.
   *
   * @returns {Promise} Promise that is resolved once the task succeeds or
   *   fails.
   */
  run (): Promise<void> {
    this._incMonitor('ponos')
    const timer = this._createTimer()
    const log = this.log.child({
      method: 'run',
      queue: this.queue,
      job: this.job
    })

    return Promise.fromCallback((cb) => {
      cls.run(() => {
        cls.set('tid', this.tid)
        Promise.try(() => {
          const attemptData = {
            attempt: this.attempt++,
            timeout: this.msTimeout
          }
          log.info(attemptData, 'running task')
          let taskPromise = Promise.try(() => {
            return this.task(this.job)
          })

          if (this.msTimeout) {
            taskPromise = taskPromise.timeout(this.msTimeout)
          }
          return taskPromise
        }).asCallback(cb)
      })
    })
    .then((result) => {
      log.info({ result: result }, 'Task complete')
      this._incMonitor('ponos.finish', { result: 'success' })
      return this.done()
    })
    // if the type is TimeoutError, we will log and retry
    .catch(TimeoutError, (err) => {
      log.warn({ err: err }, 'Task timed out')
      this._incMonitor('ponos.finish', { result: 'timeout-error' })
      // by throwing this type of error, we will retry :)
      throw err
    })
    .catch((err) => {
      if (err.cause) {
        err = err.cause
      }
      if (!isObject(err.data)) {
        err.data = {}
      }
      if (!err.data.queue) {
        err.data.queue = this.queue
      }
      if (!err.data.job) {
        err.data.job = this.job
      }
      throw err
    })
    // if it's a WorkerStopError, we can't accomplish the task
    .catch(WorkerStopError, (err) => {
      log.error({ err: err }, 'Worker task fatally errored')
      this._incMonitor('ponos.finish', { result: 'fatal-error' })
      this._reportError(err)
      // If we encounter a fatal error we should no longer try to schedule
      // the job.
      return this.done()
    })
    .catch(() => {
      // if maxNumRetries is set, call finalRetryFn and stop
      if (this.maxNumRetries && this.attempt >= this.maxNumRetries) {
        log.error({ attempt: this.attempt }, 'retry limit reached, trying handler')
        return this.finalRetryFn()
          .catch((finalErr) => {
            log.warn({ err: finalErr }, 'final function errored')
          })
          .finally(() => {
            const err = new WorkerError('final retry handler finished', {
              queue: this.queue,
              job: this.job,
              attempt: this.attempt
            })
            log.error('final retry handler finished')
            this._incMonitor('ponos.finish', { result: 'retry-error' })
            this._reportError(err)
          })
      }
    })
    .catch((err) => {
      const attemptData = {
        err: err,
        nextAttemptDelay: this.retryDelay
      }
      log.warn(attemptData, 'Task failed, retrying')
      this._incMonitor('ponos.finish', { result: 'task-error' })
      this._reportError(err)

      // Try again after a delay
      return Promise.delay(this.retryDelay)
        .then(() => {
          // Exponentially increase the retry delay
          const retryDelay = parseInt(process.env.WORKER_MAX_RETRY_DELAY) || 0
          if (this.retryDelay < retryDelay) {
            this.retryDelay *= 2
          }
          return this.run()
        })
    })
    .finally(() => {
      if (timer) {
        timer.stop()
      }
    })
  }

  // Private Methods

  /**
   * Helper function for reporting errors to rollbar via error-cat.
   *
   * @private
   * @param {Error} err Error to report.
   */
  _reportError (err: WorkerError): void {
    this.errorCat.report(err)
  }

  /**
   * Helper function for creating monitor-dog events tags. `queue` is the only
   * mandatory tag. Few tags will be created depending on the queue name. If
   * queueName use `.` as delimiter e.x. `10.0.0.20.api.github.push` then the
   * following tags will be created:
   * {
   *   token0: 'push'
   *   token1: 'github.push'
   *   token2: 'api.github.push'
   *   token3: '10.0.0.20.api.github.push'
   * }
   *
   * @private
   * @returns {Object} tags as Object { queue: 'docker.event.publish' }.
   */
  _eventTags (): Object {
    const tokens = this.queue.split('.').reverse()
    let lastToken = ''
    let tags = tokens.reduce((acc, currentValue, currentIndex) => {
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
   * Helper function calling `monitor.increment`. Monitor won't be called if
   * `WORKER_MONITOR_DISABLED` is set.
   *
   * @private
   * @param {String} eventName Name to be reported into the datadog.
   * @param {Object} [extraTags] Extra tags to be send with the event.
   */
  _incMonitor (eventName: string, extraTags?: Object): void {
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
   * Helper function calling `monitor.timer`. Timer won't be created if
   * `WORKER_MONITOR_DISABLED` is set.
   *
   * @return {Object} New timer.
   * @private
   */
  _createTimer (): ?DDTimer {
    const tags = this._eventTags()
    return !process.env.WORKER_MONITOR_DISABLED
      ? monitor.timer('ponos.timer', true, tags)
      : null
  }
}

/**
 * Worker class.
 * @module ponos/lib/worker
 * @see Worker
 */
module.exports = Worker
