'use strict';

var Promise = require('bluebird');
var TaskFatalError = require('./errors/task-fatal-error');
var TimeoutError = Promise.TimeoutError;
var assign = require('101/assign');
var defaults = require('101/defaults');
var error = require('./error');
var exists = require('101/exists');
var log = require('./logger').child({ module: 'worker' });
var merge = require('101/put');
var pick = require('101/pick');

/**
 * Worker module, for performing job tasks.
 * @author Bryan Kendall
 * @author Ryan Sandor Richards
 * @module ponos
 */
module.exports = Worker;

/**
 * Worker class: performs tasks for jobs on a given queue.
 * @class
 * @param {object} opts Options for the worker
 * @param {string} opts.queue Name of the queue for the job the worker
 *   is processing.
 * @param {function} opts.task A function to handle the tasks
 * @param {object} opts.job Data for the job to process.
 * @param {function} opts.done Callback to execute when the job has successfully
 *   been completed.
 * @param {boolean} [opts.runNow] Whether or not to run the job immediately,
 *   defaults to `true`.
 */
function Worker (opts) {
  // managed required fields
  var fields = [
    'queue',
    'task',
    'job',
    'done'
  ];
  fields.forEach(function (f) {
    if (!exists(opts[f])) {
      throw new Error(f + ' is required for a Worker');
    }
  });

  // manage field defaults
  fields.push('runNow');
  opts = pick(opts, fields);
  defaults(opts, {
    // default non-required user options
    runNow: true,
    // other options
    attempt: 0,
    msTimeout: process.env.WORKER_TIMEOUT || 0,
    retryDelay: process.env.WORKER_MIN_RETRY_DELAY || 1
  });

  // put all opts on this
  assign(this, opts);

  log.info({ queue: this.queue, job: this.job }, 'Worker created');

  if (this.runNow) {
    this.run();
  }
}

/**
 * Factory method for creating new workers. This method exists to make it easier
 * to unit test other modules that need to instantiate new workers.
 * @see Worker
 * @param {object} opts opts
 * @returns {Worker} New Worker
 */
Worker.create = function (opts) {
  return new Worker(opts);
};

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
  };
  return Promise.resolve().bind(this)
    .then(function runTheTask () {
      var attemptData = {
        attempt: this.attempt++,
        timeout: this.msTimeout
      };
      log.info(merge(jobAndQueueData, attemptData), 'Running task');
      var taskPromise = Promise.resolve().bind(this)
        .then(function () {
          return Promise.resolve().bind(this)
            .cancellable()
            .then(function () { return this.task(this.job); });
        });
      if (this.msTimeout) {
        taskPromise = taskPromise.timeout(this.msTimeout);
      }
      return taskPromise;
    })
    .then(function successDone (result) {
      log.info(merge(jobAndQueueData, { result: result }), 'Task complete');
      return this.done();
    })
    // if the type is TimeoutError, we will log and retry
    .catch(TimeoutError, function timeoutErrRetry (err) {
      log.warn(merge(jobAndQueueData, { err: err }), 'Task timed out');
      // just by throwing this type of error, we will retry :)
      throw err;
    })
    // if it's a known type of error, we can't accomplish the task
    .catch(TaskFatalError, function knownErrDone (err) {
      error.log(err);
      // If we encounter a fatal error we should no longer try to schedule
      // the job.
      return this.done();
    })
    .catch(function unknownErrRetry (err) {
      var attemptData = {
        err: err,
        nextAttemptDelay: this.retryDelay
      };
      log.warn(merge(jobAndQueueData, attemptData), 'Task failed, retrying');

      // Try again after a delay
      return Promise.delay(this.retryDelay).bind(this)
        .then(function retryRun () {
          // Exponentially increase the retry delay
          if (this.retryDelay < process.env.WORKER_MAX_RETRY_DELAY) {
            this.retryDelay *= 2;
          }
          return this.run();
        });
    });
};
