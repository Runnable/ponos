'use strict'

const chai = require('chai')

const assert = chai.assert

// Ponos Tooling
const ponos = require('../../')
const testWorker = require('./fixtures/worker')
const testWorkerEmitter = testWorker.emitter

describe('Basic Example', () => {
  var server
  before((done) => {
    var tasks = {
      'ponos-test:one': testWorker
    }
    server = new ponos.Server({ queues: Object.keys(tasks) })
    server.setAllTasks(tasks).start().asCallback(done)
  })
  after((done) => { server.stop().then(() => { done() }) })

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
