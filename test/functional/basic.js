'use strict'

const chai = require('chai')

const assert = chai.assert

// Ponos Tooling
const ponos = require('../../src')
const RabbitMQ = require('../../src/rabbitmq')
const basicWorker = require('./fixtures/worker')
const basicWorkerTwoEmitter = basicWorker.emitter
const testWorkerOne = require('./fixtures/worker-one')
const testWorkerTwo = require('./fixtures/worker-two')
const testWorkerTwoEmitter = testWorkerTwo.emitter

describe('Basic Example', () => {
  let server
  let rabbitmq
  const testQueueBasic = 'ponos-test:zero'
  const testQueueOne = 'ponos-test:one'
  const testQueueTwo = 'ponos-test:two'
  before(() => {
    const tasks = {}
    rabbitmq = new RabbitMQ({
      name: 'ponos.test',
      tasks: [ testQueueBasic, testQueueOne, testQueueTwo ]
    })
    tasks[testQueueBasic] = basicWorker
    tasks[testQueueOne] = testWorkerOne
    tasks[testQueueTwo] = testWorkerTwo
    server = new ponos.Server({ name: 'ponos.test', tasks: tasks })
    return rabbitmq.connect()
      .then(() => {
        testWorkerOne.setPublisher(rabbitmq)
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
    basicWorkerTwoEmitter.on('task', function (job, jobMeta) {
      assert.equal(job.message, 'hello world')
      done()
    })
    const job = {
      eventName: 'task',
      message: 'hello world'
    }
    rabbitmq.publishTask(testQueueBasic, job)
  })

  it('should trigger series of events', (done) => {
    testWorkerTwoEmitter.on('task', function (job, jobMeta) {
      assert.equal(jobMeta.headers.publisherWorkerName, testQueueOne)
      assert.equal(job.message, 'hello world2')
      done()
    })
    const job = {
      eventName: 'task',
      message: 'hello world'
    }
    rabbitmq.publishTask(testQueueOne, job)
  })
})
