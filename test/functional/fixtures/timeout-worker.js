'use strict'

const EventEmitter = require('events')
const Promise = require('bluebird')

const ponos = require('../../../')
const TaskFatalError = ponos.TaskFatalError

/**
 * A worker that will publish a message to an event emitter. This worker also
 * will take a long time to do so (causing timeouts). It has a _timeout value
 * below that is halved every run to provide a hastening task.
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
      var timeout = module.exports._timeout
      // every time this worker is run, it will halve it's delay (run) time
      module.exports._timeout = module.exports._timeout / 2
      return Promise.resolve()
        .delay(timeout)
        .then(() => {
          module.exports.emitter.emit(job.eventName, { data: job.message })
          return true
        })
    })
}

// this _timeout value is the starting value for how long this worker will take
module.exports._timeout = 3000
module.exports.emitter = new EventEmitter()
