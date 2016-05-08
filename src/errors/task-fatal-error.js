'use strict'

const TaskError = require('./task-error')

/**
 * Error type for tasks that indicate a job cannot be fulfilled. When workers
 * encounter this type of error they should immediately stop trying to fulfill
 * a job via the task.
 * @class
 * @param {string} queue Name of the queue that encountered the error.
 * @param {string} message Message for the error.
 * @param {object} [data] Additional data for the error, optional.
 */
class TaskFatalError extends TaskError {}

/**
 * Fatal error that indicates a job should be abandoned.
 * @author Ryan Sandor Richards
 * @module ponos:errors
 */
module.exports = TaskFatalError
