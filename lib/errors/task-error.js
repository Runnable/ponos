'use strict'

const defaults = require('101/defaults')
const ExtendableError = require('es6-error')
const isObject = require('101/is-object')

/**
 * Error type for task promise rejection that indicates to the worker that
 * something went wrong, but that it should attempt the task again. Useful for
 * handling any sort of issue that may have a temporal component (network
 * connectivity, etc.)
 * @class
 * @param {string} queue Name of the queue that encountered the error.
 * @param {string} message Message for the task error.
 * @param {object} [data] Extra data to include with the error, optional.
 */
class TaskError extends ExtendableError {
  constructor (queue, message, data) {
    const newMessage = queue + ': ' + message
    super(newMessage)
    let errorData = { queue: queue }
    if (isObject(data)) {
      defaults(errorData, data)
    }
    this.data = errorData
  }
}

/**
 * Normal error for tasks that indicates the job should be tried again.
 * @author Ryan Sandor Richards
 * @module ponos:errors
 */
module.exports = TaskError
