'use strict'

const WorkerError = require('error-cat/errors/worker-error')

/**
 * Error type for task promise rejection that indicates to the worker that
 * something went wrong, but that it should attempt the task again. Useful for
 * handling any sort of issue that may have a temporal component (network
 * connectivity, etc.)
 * @class
 * @author Ryan Sandor Richards
 * @module ponos:errors
 */
module.exports = class TaskError extends WorkerError {
  /**
   * Creates new TaskError.
   * @param {string} queue Name of the queue that encountered the error.
   * @param {string} message Message for the task error.
   * @param {object} job Job that caused the error.
   * @param {object} [data] Extra data to include with the error.
   */
  constructor (queue, message, job, data) {
    const newMessage = queue + ': ' + message
    // WorkerError(message, data, queue, job)
    super(newMessage, data, queue, job)
  }
}
