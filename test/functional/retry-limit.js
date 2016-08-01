'use strict'

const chai = require('chai')
const sinon = require('sinon')
const Promise = require('bluebird')

const assert = chai.assert

// Ponos Tooling
const ponos = require('../../src')
const RabbitMQ = require('../../src/rabbitmq')

// require the Worker class so we can verify the task is running
const _Worker = require('../../src/worker')
/*
 *  In this example, we are going to have a job handler that times out at
 *  decreasing intervals, throwing TimeoutErrors, until it passes.
 */
describe('Basic Timeout Task', function () {
  let server
  let rabbitmq
  let testRecover
  before(() => {
    sinon.spy(_Worker.prototype, 'run')
    rabbitmq = new RabbitMQ({})
    const tasks = {
      'ponos-test:one': {
        task: () => {
          return Promise.reject(new Error('death to all'))
        },
        finalRetryFn: (job) => {
          return testRecover(job)
        },
        maxNumRetries: 5
      }
    }
    server = new ponos.Server({ tasks: tasks })
    return server.start()
      .then(() => {
        return rabbitmq.connect()
      })
  })
  after(() => {
    _Worker.prototype.run.restore()
    return server.stop()
      .then(() => {
        return rabbitmq.disconnect()
      })
  })

  const job = {
    message: 'hello world'
  }

  describe('with maxNumRetries', function () {
    it('should fail 4 times and pass the fifth time', (done) => {
      testRecover = function (testJob) {
        try {
          assert.equal(job.message, testJob.message)
          sinon.assert.callCount(_Worker.prototype.run, 5)
        } catch (err) {
          return done(err)
        }
        done()
        return Promise.resolve()
      }
      rabbitmq.publishTask('ponos-test:one', job)
    })
  })
})
