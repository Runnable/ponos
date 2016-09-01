/* @flow */
/* global Logger DDTimer */
'use strict'

const cls = require('continuation-local-storage').createNamespace('ponos')
const clsBlueBird = require('@runnable/cls-bluebird')
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
  attempt: joi.number().integer().min(0).required(),
  done: joi.func().required(),
  errorCat: joi.object(),
  finalRetryFn: joi.func(),
  jobSchema: joi.object({
    isJoi: joi.bool().valid(true)
  }).unknown(),
  job: joi.object().required(),
  log: joi.object().required(),
  maxNumRetries: joi.number().integer().min(0).required(),
  msTimeout: joi.number().integer().min(0).required(),
  queue: joi.string().required(),
  retryDelay: joi.number().integer().min(1).required(),
  maxRetryDelay: joi.number().integer().min(0).required(),
  task: joi.func().required()
}).unknown()

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
 */
class Worker {
  attempt: number;
  done: Function;
  errorCat: ErrorCat;
  finalRetryFn: Function;
  jobSchema: Object;
  job: Object;
  log: Logger;
  maxNumRetries: number;
  msTimeout: number;
  queue: String;
  retryDelay: number;
  maxRetryDelay: number
  task: Function;
  tid: String;

  constructor (opts: Object) {
    defaults(opts, {
      // default non-required user options
      errorCat: ErrorCat,
      // other options
      attempt: 0,
      finalRetryFn: () => { return Promise.resolve() },
      maxNumRetries: parseInt(process.env.WORKER_MAX_NUM_RETRIES, 10) || Number.MAX_SAFE_INTEGER,
      msTimeout: parseInt(process.env.WORKER_TIMEOUT, 10) || 0,
      maxRetryDelay: parseInt(process.env.WORKER_MAX_RETRY_DELAY, 10) || Number.MAX_SAFE_INTEGER,
      retryDelay: parseInt(process.env.WORKER_MIN_RETRY_DELAY, 10) || 1
    })
    // managed required fields
    joi.assert(opts, optsSchema)
    this.tid = opts.job.tid || uuid()
    opts.log = opts.log.child({ tid: this.tid, module: 'ponos:worker' })
    // put all opts on this
    Object.assign(this, opts)
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
   * validate job against schema if passed
   * @return {[type]} [description]
   */
  _validateJob (): Promise<void> {
    return Promise.try(() => {
      if (this.jobSchema) {
        joi.assert(this.job, this.jobSchema)
      }
    })
    .catch((err) => {
      throw new WorkerStopError('Invalid job', {
        queue: this.queue,
        job: this.job,
        validationErr: err
      })
    })
  }
  /**
   * Wraps tasks with CLS and timeout
   * @returns {Promise}
   * @resolves {Object} when task is complete
   * @rejects {Error} if job errored
   */
  _wrapTask (): Promise<any> {
    return Promise.fromCallback((cb) => {
      cls.run(() => {
        cls.set('tid', this.tid)
        Promise.try(() => {
          this.log.info({
            attempt: this.attempt++,
            timeout: this.msTimeout
          }, 'running task')
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
  }

  /**
   * adds worker properties to error
   * @param  {Error} err error to augment
   * @throws {Error}     error with extra data
   */
  _addDataToError (err: Object) {
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
  }

  /**
   * retry task with delay function
   * @param  {Error} err error that is causing retry
   * @return {Promise}
   * @resolves {Object} when task is resolved
   */
  _retryWithDelay (err: Object) {
    this.log.warn({
      err: err,
      nextAttemptDelay: this.retryDelay,
      attemptCount: this.attempt
    }, 'Task failed, retrying')
    this._incMonitor('ponos.finish', { result: 'task-error' })

    // Try again after a delay
    return Promise.delay(this.retryDelay)
      .then(() => {
        // Exponentially increase the retry delay to max
        if (this.retryDelay < this.maxRetryDelay) {
          this.retryDelay *= 2
        }
        return this.run()
      })
  }

  /**
   * throw Worker Stop Error if we reached retry limit
   * @param  {Error} err error that worker threw
   * @return {Promise}
   * @resolves should never resolve
   * @rejects {Error} when attempt limit not reached
   * @rejects {WorkerStopErrpr} when attempt limit reached
   */
  _enforceRetryLimit (err: Object) {
    if (this.attempt < this.maxNumRetries) {
      return Promise.reject(err)
    }

    this.log.error({
      attempt: this.attempt,
      maxNumRetries: this.maxNumRetries
    }, 'retry limit reached, trying handler')

    return Promise.try(() => {
      return this.finalRetryFn(this.job)
    })
    .catch((finalErr) => {
      this._incMonitor('ponos.finish-retry-fn-error', { result: 'retry-fn-error' })
      this.log.warn({ err: finalErr }, 'final function errored')
    })
    .finally(() => {
      this._incMonitor('ponos.finish-error', { result: 'retry-error' })
      throw new WorkerStopError('final retry handler finished', {
        queue: this.queue,
        job: this.job,
        attempt: this.attempt
      })
    })
  }

  /**
   * Do not propagate error and log
   * @param  {WorkerStopError} err error that caused worker to stop
   * @return {undefined}
   */
  _handleWorkerStopError (err: Object) {
    this.log.error({ err: err }, 'Worker task fatally errored')
    this._incMonitor('ponos.finish-error', { result: 'fatal-error' })
  }

  /**
   * Propagate error and log
   * @param  {TimeoutError} err error that caused worker to stop
   * @return {undefined}
   */
  _handleTimeoutError (err: Object) {
    this.log.warn({ err: err }, 'Task timed out')
    this._incMonitor('ponos.finish-error', { result: 'timeout-error' })
    // by throwing this type of error, we will retry :)
    throw err
  }

  /**
   * log task complete
   * @return {undefined}
   */
  _handleTaskSuccess () {
    this.log.info('Task complete')
    this._incMonitor('ponos.finish', { result: 'success' })
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
    this.log = this.log.child({
      method: 'run',
      queue: this.queue,
      job: this.job
    })

    return this._validateJob()
      .then(() => {
        return this._wrapTask()
      })
      .then(() => {
        return this._handleTaskSuccess()
      })
      .catch((err) => {
        return this._addDataToError(err)
      })
      // if the type is TimeoutError, we will log and retry
      .catch(TimeoutError, (err) => {
        return this._handleTimeoutError(err)
      })
      .catch((err) => {
        return this._enforceRetryLimit(err)
      })
      // if it's a WorkerStopError, we stop this task
      .catch((err) => {
        this.errorCat.report(err)
        throw err
      })
      .catch(WorkerStopError, (err) => {
        return this._handleWorkerStopError(err)
      })
      .catch((err) => {
        return this._retryWithDelay(err)
      })
      .finally(() => {
        if (timer) {
          timer.stop()
        }
        return this.done()
      })
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
