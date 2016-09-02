'use strict'

const chai = require('chai')
const Promise = require('bluebird')
const sinon = require('sinon')
const WorkerStopError = require('error-cat/errors/worker-stop-error')
const ErrorCat = require('error-cat')
const assert = chai.assert

// Ponos Tooling
const ponos = require('../../src')
const RabbitMQ = require('../../src/rabbitmq')
const testWorker = require('./fixtures/worker')
const testWorkerEmitter = testWorker.emitter

// require the Worker class so we can verify the task is running
const _Worker = require('../../src/worker')

/*
 *  In this example, we are going to pass an invalid job to the worker that will
 *  throw a WorkerStopError, acknowledge the job, and not run it a second time.
 */
describe('Basic Failing Task', () => {
  let server
  let rabbitmq

  before(() => {
    sinon.spy(_Worker.prototype, 'run')
    sinon.spy(ErrorCat, 'report')
    const tasks = {
      'ponos-test:one': testWorker
    }
    rabbitmq = new RabbitMQ({
      tasks: Object.keys(tasks)
    })
    server = new ponos.Server({
      tasks: tasks
    })
    return rabbitmq.connect()
      .then(() => {
        return server.start()
      })
  })

  after(() => {
    _Worker.prototype.run.restore()
    ErrorCat.report.restore()
    return server.stop()
      .then(() => {
        return rabbitmq.disconnect()
      })
  })

  const job = {
    eventName: 'will-never-emit'
  }

  // Before we run the test, let's assert that our task fails with the job.
  // This should be _rejected_ with an error.
  before(() => {
    return assert.isRejected(
      testWorker(job),
      WorkerStopError,
      /message.+required/
    )
  })

  it('should fail once and not be re-run', () => {
    testWorkerEmitter.on('will-never-emit', () => {
      throw new Error('failing worker should not have emitted')
    })
    rabbitmq.publishTask('ponos-test:one', job)

    // wait until .run is called
    return Promise.try(function loop () {
      if (!_Worker.prototype.run.calledOnce) {
        return Promise.delay(5).then(loop)
      }
    })
    .then(() => {
      assert.ok(_Worker.prototype.run.calledOnce, '.run called once')
      /*
       *  We can get the promise and assure that it was fulfilled!
       *  This should be _fulfilled_ because it threw a WorkerStopError and
       *  acknowledged that the task was completed (even though the task
       *  rejected with an error)
       */
      const workerRunPromise = _Worker.prototype.run.firstCall.returnValue
      assert.isFulfilled(workerRunPromise)
      assert.ok(
        ErrorCat.report.calledOnce,
        'worker.report called once'
      )
      const err = ErrorCat.report.firstCall.args[0]
      assert.instanceOf(err, WorkerStopError)
      assert.match(err, /fail test message is required/)

      return Promise.resolve()
    })
  })
})
