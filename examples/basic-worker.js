'use strict'

var getNamespace = require('continuation-local-storage').getNamespace
var Promise = require('bluebird')
var WorkerStopError = require('error-cat/errors/worker-stop-error')

var Ponos = require('../')

/**
 * A simple worker that will publish a message to a queue.
 * @param {object} job Object describing the job.
 * @param {string} job.queue Queue on which the message will be published.
 * @returns {promise} Resolved when the message is put on the queue.
 */
function basicWorker (job) {
  return Promise.try(function () {
    var tid = getNamespace('ponos').get('tid')
    if (!job.message) {
      throw new WorkerStopError('message is required', { tid: tid })
    }
  })
}

var server = new Ponos.Server({
  tasks: {
    'basic-queue-worker': basicWorker
  },
  events: {
    'basic-event-worker': basicWorker
  }
})

server.start()
  .then(function () { console.log('server started') })
  .catch(function (err) { console.error('server error:', err.stack || err.message || err) })

process.on('SIGINT', function () {
  server.stop()
    .then(function () { console.log('server stopped') })
    .catch(function (err) { console.error('server error:', err.stack || err.message || err) })
})
