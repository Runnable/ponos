'use strict'
const chai = require('chai')
const Promise = require('bluebird')

const ponos = require('../../src')
const RabbitMQ = require('../../src/rabbitmq')

const assert = chai.assert

describe('Basic Example', () => {
  let server
  let rabbitmq
  let count = 0
  const testQueue = 'ponos-test:rate-limit'
  before(() => {
    const tasks = {}
    tasks[testQueue] = {
      task: () => {
        return Promise.try(() => {
          count++
        })
      },
      durationMs: 2000,
      maxOperations: 10
    }
    rabbitmq = new RabbitMQ({
      tasks: Object.keys(tasks)
    })
    server = new ponos.Server({
      tasks: tasks,
      redisRateLimiter: {}
    })
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

  it('should queue a task that triggers an event', () => {
    const job = {
      eventName: 'task',
      message: 'hello world'
    }

    return Promise.try(() => {
      for (var i = 0; i < 20; i++) {
        rabbitmq.publishTask(testQueue, job)
      }
    })
    .delay(1000)
    .then(() => {
      assert.equal(10, count)
    })
    .delay(2000)
    .then(() => {
      assert.equal(20, count)
    })
  })
})

// set rate
// set timeout for half, ensure half ran
// set timeout for all, ensure all ran
