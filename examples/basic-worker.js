'use strict'

const Promise = require('bluebird')
const WorkerStopError = require('error-cat/errors/worker-stop-error')

const Ponos = require('../')

/**
 * A simple worker that will publish a message to a queue.
 * @param {object} job Object describing the job.
 * @param {string} job.queue Queue on which the message will be published.
 * @returns {promise} Resolved when the message is put on the queue.
 */
function basicWorker (job) {
  return Promise.resolve()
    .then(() => {
      if (!job.message) {
        throw new WorkerStopError('queue', 'message is required')
      }
      console.log(`hello world: ${job.message}`)
    })
}

const server = new Ponos.Server({
  tasks: {
    'basic-queue-worker': basicWorker
  },
  events: {
    'basic-event-worker': basicWorker
  }
})

server.start()
  .then(() => { console.log('server started') })
  .catch((err) => { console.error('server error:', err.stack || err.message || err) })

process.on('SIGINT', () => {
  server.stop()
    .then(() => { console.log('server stopped') })
    .catch((err) => { console.error('server error:', err.stack || err.message || err) })
})
