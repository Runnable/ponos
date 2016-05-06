'use strict'

const chai = require('chai')
const sinon = require('sinon')

const assert = chai.assert

// Ponos Tooling
const ponos = require('../../')
const TimeoutError = require('bluebird').TimeoutError
const testWorker = require('./fixtures/timeout-worker')
const testWorkerEmitter = testWorker.emitter

// require the Worker class so we can verify the task is running
const _Worker = require('../../lib/worker')
// require the error module so we can see the error printed
const _Bunyan = require('bunyan')

/*
 *  In this example, we are going to have a job handler that times out at
 *  decreasing intervals, throwing TimeoutErrors, until it passes.
 */
describe('Basic Timeout Task', function () {
  let server
  before(() => {
    sinon.spy(_Worker.prototype, 'run')
    sinon.spy(_Bunyan.prototype, 'warn')
    const tasks = {
      'ponos-test:one': testWorker
    }
    server = new ponos.Server({ tasks: tasks })
    return server.setAllTasks(tasks).start()
  })
  after(() => {
    _Worker.prototype.run.restore()
    _Bunyan.prototype.warn.restore()
    return server.stop()
  })

  const job = {
    eventName: 'did-not-time-out',
    message: 'hello world'
  }

  describe('with a timeout', function () {
    this.timeout(3500)
    let prevTimeout

    before(() => {
      prevTimeout = process.env.WORKER_TIMEOUT
      process.env.WORKER_TIMEOUT = 1000
    })

    after(() => {
      process.env.WORKER_TIMEOUT = prevTimeout
    })

    it('should fail twice and pass the third time', (done) => {
      testWorkerEmitter.on('did-not-time-out', () => {
        // process.nextTick so the worker can resolve
        // NOTE(bryan): I found nextTick to be more consistant than setTimeout
        process.nextTick(() => {
          // this signals to us that we are done!
          assert.ok(_Worker.prototype.run.calledThrice, '.run called thrice')
          /*
           *  We can get the promise and assure that it was fulfilled!
           *  It was run three times and all three should be fulfilled.
           */
          ;[
            _Worker.prototype.run.firstCall.returnValue,
            _Worker.prototype.run.secondCall.returnValue,
            _Worker.prototype.run.thirdCall.returnValue
          ].forEach(function (p) { assert.isFulfilled(p) })
          /*
           * and, make sure the error module has logged the TimeoutError twice
           * have to do a bit of weird filtering, but this is correct. Long
           * story short, log.warn is called a couple times, but we just want to
           * make sure the 'task timed out' message is just twice (the number of
           * times this worker failed).
           */
          const bunyanCalls = _Bunyan.prototype.warn.args
          const errors = bunyanCalls.reduce(function (memo, args) {
            const checkArgs = args.filter(function (arg) {
              return /task timed out/i.test(arg)
            })
            if (checkArgs.length) { memo.push(args.shift().err) }
            return memo
          }, [])
          errors.forEach(function (err) {
            assert.instanceOf(err, TimeoutError)
          })
          done()
        })
      })

      server._rabbitmq.channel.sendToQueue('ponos-test:one', new Buffer(JSON.stringify(job)))
    })
  })
})
