'use strict'

const WorkerStopError = require('error-cat/errors/worker-stop-error')

/**
 * Error type for tasks that indicate a job cannot be fulfilled. When workers
 * encounter this type of error they should immediately stop trying to fulfill
 * a job via the task.
 * @class
 * @author Ryan Sandor Richards
 * @module ponos:errors
 */
module.exports = class TaskFatalError extends WorkerStopError {
  /**
  * Creates new TaskFatalError.
  * @param {string} queue Name of the queue that encountered the error.
  * @param {string} message Message for the error.
  * @param {object} job Job that caused the error.
  * @param {object} [data] Additional data for the error, optional.
  */
  constructor (queue, message, job, data) {
    const newMessage = queue + ': ' + message
    // WorkerError(message, data, queue, job)
    super(newMessage, data, queue, job)
  }
}
