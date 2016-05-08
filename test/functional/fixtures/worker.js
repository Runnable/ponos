'use strict'

const EventEmitter = require('events')
const Promise = require('bluebird')

const ponos = require('../../../src')
const TaskFatalError = ponos.TaskFatalError

/**
 * A simple worker that will publish a message to a queue.
 * @param {object} job Object describing the job.
 * @param {string} job.queue Queue on which the message will be published.
 * @returns {promise} Resolved when the message is put on the queue.
 */
module.exports = (job) => {
  return Promise.resolve()
    .then(() => {
      if (!job.eventName) { throw new TaskFatalError('queue', 'eventName is required') }
      if (!job.message) { throw new TaskFatalError('queue', 'message is required') }
    })
    .then(() => {
      module.exports.emitter.emit(job.eventName, { data: job.message })
    })
}

module.exports.emitter = new EventEmitter()
