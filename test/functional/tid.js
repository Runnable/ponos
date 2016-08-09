'use strict'

const testWorker = require('./fixtures/worker-tid')
const testWorkerEmitter = testWorker.emitter

// Ponos Tooling
const ponos = require('../../src')
const RabbitMQ = require('../../src/rabbitmq')

const Promise = require('bluebird')


describe('Basic Example', () => {
  let server
  let rabbitmq

  before(() => {
    rabbitmq = new RabbitMQ({})
    const tasks = {
      'ponos-test:one': testWorker
    }
    server = new ponos.Server({ tasks: tasks })
    return server.start()
      .then(() => {
        return rabbitmq.connect()
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
