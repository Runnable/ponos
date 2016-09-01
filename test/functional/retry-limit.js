'use strict'
const sinon = require('sinon')
const Promise = require('bluebird')

// Ponos Tooling
const ponos = require('../../src')
const RabbitMQ = require('../../src/rabbitmq')

/*
 *  In this example, we are going to have a job handler that times out at
 *  decreasing intervals, throwing TimeoutErrors, until it passes.
 */
describe('Retry limit task', function () {
  let server
  let rabbitmq
  const testRecoverStub = sinon.stub().resolves()
  const taskStub = sinon.stub().rejects(new Error('death to all'))
  before(() => {
    const tasks = {
      'ponos-test:one': {
        task: taskStub,
        finalRetryFn: testRecoverStub,
        maxNumRetries: 3
      }
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

  const job = {
    message: 'hello world',
    tid: 'test-tid'
  }

  describe('with maxNumRetries', function () {
    it('should fail 3 times and run recovery function', () => {
      rabbitmq.publishTask('ponos-test:one', job)

      return Promise.try(function loop () {
        if (taskStub.callCount !== 3) {
          return Promise.delay(3).then(loop)
        }
      })
      .then(() => {
        sinon.assert.calledOnce(testRecoverStub)
        sinon.assert.calledWith(testRecoverStub, job)
        sinon.assert.callCount(taskStub, 3)
        sinon.assert.alwaysCalledWithExactly(taskStub, job)
      })
    })
  })
})
