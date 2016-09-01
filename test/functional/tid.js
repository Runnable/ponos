'use strict'

const testWorker = require('./fixtures/worker-tid')
const testWorkerEmitter = testWorker.emitter

// Ponos Tooling
const ponos = require('../../src')
const RabbitMQ = require('../../src/rabbitmq')

describe('Basic Example', () => {
  let server
  let rabbitmq

  before(() => {
    const tasks = {
      'ponos-test:one': testWorker
    }
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
    const job = {
      eventName: 'task',
      message: 'hello world'
    }
    testWorkerEmitter.on('failed', function (err) {
      done(new Error(err.message))
    })
    testWorkerEmitter.on('passed', function () {
      done()
    })
    rabbitmq.publishTask('ponos-test:one', job)
  })
})
