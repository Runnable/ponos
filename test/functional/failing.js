'use strict'

const chai = require('chai')
const async = require('async')
const sinon = require('sinon')

const assert = chai.assert

// Ponos Tooling
const ponos = require('../../')
const TaskFatalError = ponos.TaskFatalError
const testWorker = require('./fixtures/worker')
const testWorkerEmitter = testWorker.emitter

// require the Worker class so we can verify the task is running
const _Worker = require('../../lib/worker')

/*
 *  In this example, we are going to pass an invalid job to the worker that will
 *  throw a TaskFatalError, acknowledge the job, and not run it a second time.
 */
describe('Basic Failing Task', () => {
  let server
  before((done) => {
    sinon.spy(_Worker.prototype, 'run')
    sinon.spy(_Worker.prototype, '_reportError')
    const tasks = {
      'ponos-test:one': testWorker
    }
    server = new ponos.Server({ queues: Object.keys(tasks) })
    server.setAllTasks(tasks).start()
      .then(() => {
        assert.notOk(_Worker.prototype.run.called, '.run should not be called')
        done()
      })
      .catch(done)
  })
  after((done) => {
    server.stop()
      .then(() => {
        _Worker.prototype.run.restore()
        _Worker.prototype._reportError.restore()
        done()
      })
  })

  const job = {
    eventName: 'will-never-emit'
  }

  // Before we run the test, let's assert that our task fails with the job.
  // This should be _rejected_ with an error.
  before(() => {
    const testWorkerPromise = testWorker(job)
      .catch((err) => {
        // extra assertion that it's still a TaskFatalError
        assert.instanceOf(err, TaskFatalError)
        throw err
      })
    return assert.isRejected(testWorkerPromise, /message.+required/)
  })

  it('should fail once and not be re-run', (done) => {
    testWorkerEmitter.on('will-never-emit', () => {
      done(new Error('failing worker should not have emitted'))
    })
    server.hermes.publish('ponos-test:one', job)

    // wait until .run is called
    async.until(
      () => { return _Worker.prototype.run.calledOnce },
      (cb) => { setTimeout(cb, 5) },
      () => {
        assert.ok(_Worker.prototype.run.calledOnce, '.run called once')
        /*
         *  We can get the promise and assure that it was fulfilled!
         *  This should be _fulfilled_ because it threw a TaskFatalError and
         *  acknowledged that the task was completed (even though the task
         *  rejected with an error)
         */
        const workerRunPromise = _Worker.prototype.run.firstCall.returnValue
        assert.isFulfilled(workerRunPromise)
        assert.ok(
          _Worker.prototype._reportError.calledOnce,
          'worker._reportError called once'
        )
        const err = _Worker.prototype._reportError.firstCall.args[0]
        assert.instanceOf(err, TaskFatalError)
        assert.match(err, /message.+required/)
        done()
      })
  })
})
