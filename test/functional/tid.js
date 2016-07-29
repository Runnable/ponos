'use strict'

const chai = require('chai')

const assert = chai.assert

// Ponos Tooling
const ponos = require('../../src')
const RabbitMQ = require('../../src/rabbitmq')
const testWorker = require('./fixtures/worker')
const testWorkerEmitter = (job) => {
  return Promise
    .try(() => {

    })
    .then(() => {
      console.log('hi', job)
    })
}

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
    rabbitmq.publishTask('ponos-test:one', job)
  })
})
