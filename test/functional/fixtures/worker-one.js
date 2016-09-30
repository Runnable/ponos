'use strict'

const EventEmitter = require('events')
const Promise = require('bluebird')
const WorkerStopError = require('error-cat/errors/worker-stop-error')

let rabbitmq
/**
 * A simple worker that will publish a message to a queue.
 * @param {object} job Object describing the job.
 * @param {string} job.queue Queue on which the message will be published.
 * @returns {promise} Resolved when the message is put on the queue.
 */
module.exports = (job, jobMeta) => {
  return Promise.resolve()
    .then(() => {
      if (!job.eventName) {
        throw new WorkerStopError('eventName is required')
      }
      if (!job.message) {
        throw new WorkerStopError('fail test message is required')
      }
    })
    .then(() => {
      const job = {
        eventName: 'task',
        message: 'hello world2'
      }
      rabbitmq.publishTask('ponos-test:two', job)
      module.exports.emitter.emit(job.eventName, job, jobMeta)
    })
}

module.exports.setPublisher = (publisher) => {
  rabbitmq = publisher
}
module.exports.emitter = new EventEmitter()
