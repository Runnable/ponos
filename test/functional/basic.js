'use strict'

const chai = require('chai')

const assert = chai.assert

// Ponos Tooling
const ponos = require('../../')
const testWorker = require('./fixtures/worker')
const testWorkerEmitter = testWorker.emitter

describe('Basic Example', () => {
  var server

  before(() => {
    var tasks = {
      'ponos-test:one': testWorker
    }
    server = new ponos.Server({ queues: Object.keys(tasks) })
    return server.setAllTasks(tasks).start()
  })

  after(() => {
    return server.stop()
  })

  it('should queue a task that triggers an event', (done) => {
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
