'use strict';

var Promise = require('bluebird');
var TaskFatalError = require('./errors/task-fatal-error');
var TaskMaxRetriesError = require('./errors/task-max-retries-error');
var assign = require('101/assign');
var defaults = require('101/defaults');
var error = require('./error');
var exists = require('101/exists');
var log = require('./logger').child({ module: 'worker' });
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
    runNow: true,
    // not necessarily user opts
    attempt: 0,
    retryDelay: process.env.WORKER_MIN_RETRY_DELAY
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
  var baseLogData = {
    queue: this.queue,
    job: this.job
  };
  return Promise.resolve().bind(this)
    .then(function checkAttemptCounter () {
      this.attempt++;
      // Check if we've exceeded the maximum number of retries
      if (this.attempt > process.env.WORKER_MAX_RETRIES) {
        // TODO(bryan): add negative acknowledgements here when they are ready
        var errData = {
          job: this.job,
          attempts: this.attempt
        };
        throw new TaskMaxRetriesError(this.queue, errData);
      }
    })
    .then(function runTheTask () {
      var attemptData = { attempt: this.attempt };
      log.info(assign({}, baseLogData, attemptData), 'Running task');
      return this.task(this.job);
    })
    .then(function successDone (result) {
      log.info(assign({}, baseLogData, { result: result }), 'Job complete');
      return this.done();
    })
    // if it's a known type of error, we can't accomplish the task
    .catch(TaskFatalError, TaskMaxRetriesError, function knownErrDone (err) {
      error.log(err);
      // If we encounter a fatal error we should no longer try to schedule
      // the job.
      return this.done();
    })
    // TODO(bryan): do we want to do something special for TaskError?
    .catch(function unknownErrRetry () {
      var attemptData = { nextAttemptDelay: this.retryDelay };
      log.warn(assign({}, baseLogData, attemptData), 'Task failed, retrying');

      // Try again after a delay
      return Promise.resolve()
        .delay(this.retryDelay).bind(this)
        .then(function () {
          // Exponentially increase the retry delay
          if (this.retryDelay < process.env.WORKER_MAX_RETRY_DELAY) {
            this.retryDelay *= 2;
          }
          return this.run();
        });
    });
};