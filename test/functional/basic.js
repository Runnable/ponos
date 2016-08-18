'use strict'

const chai = require('chai')

const assert = chai.assert

// Ponos Tooling
const ponos = require('../../src')
const RabbitMQ = require('../../src/rabbitmq')
const testWorker = require('./fixtures/worker')
const testWorkerEmitter = testWorker.emitter

describe('Basic Example', () => {
  let server
  let rabbitmq
  const testQueue = 'ponos-test:one'
  before(() => {
    const tasks = {}
    tasks[testQueue] = testWorker
    rabbitmq = new RabbitMQ({
      tasks: Object.keys(tasks)
    })
    server = new ponos.Server({ tasks: tasks })
    return rabbitmq.connect()
      .then(() => {
        return server.start()
      })
  })

  after(() => {
    return server.stop()
      .then(() => {
        return rabbitmq.disconnect()
      })
  })

  it('should queue a task that triggers an event', (done) => {
    testWorkerEmitter.on('task', function (data) {
      assert.equal(data.data, 'hello world')
      done()
    })
    const job = {
      eventName: 'task',
      message: 'hello world'
    }
    rabbitmq.publishTask(testQueue, job)
  })
})
