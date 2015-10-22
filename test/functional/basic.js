'use strict'

var chai = require('chai')
var assert = chai.assert

// Ponos Tooling
var ponos = require('../../')
var testWorker = require('./fixtures/worker')
var testWorkerEmitter = testWorker.emitter

describe('Basic Example', function () {
  var server
  before(function (done) {
    var tasks = {
      'ponos-test:one': testWorker
    }
    server = new ponos.Server({ queues: Object.keys(tasks) })
    server.setAllTasks(tasks).start().asCallback(done)
  })
  after(function (done) { server.stop().then(function () { done() }) })

  it('should queue a task that triggers an event', function (done) {
    testWorkerEmitter.on('task', function (data) {
      assert.equal(data.data, 'hello world')
      done()
    })
    var job = {
      eventName: 'task',
      message: 'hello world'
    }
    server.hermes.publish('ponos-test:one', job)
  })
})
