'use strict'

const assign = require('101/assign')
const chai = require('chai')
const monitor = require('monitor-dog')
const noop = require('101/noop')
const omit = require('101/omit')
const Promise = require('bluebird')
const put = require('101/put')
const sinon = require('sinon')

const assert = chai.assert
const TimeoutError = Promise.TimeoutError

const TaskError = require('../../lib/errors/task-error')
const TaskFatalError = require('../../lib/errors/task-fatal-error')
const Worker = require('../../lib/worker')

describe('Worker', () => {
  let opts
  let taskHandler
  let doneHandler

  beforeEach(() => {
    opts = {
      queue: 'do.something.command',
      task: (data) => { return Promise.resolve(data).then(taskHandler) },
      job: { message: 'hello world' },
      done: () => { return Promise.resolve().then(doneHandler) }
    }
  })

  describe('Constructor', () => {
    beforeEach(() => { sinon.stub(Worker.prototype, 'run') })

    afterEach(() => { Worker.prototype.run.restore() })

    it('should enforce default opts', () => {
      const testOpts = omit(opts, 'job')
      assert.throws(() => {
        Worker.create(testOpts)
      }, /job is required.+Worker/)
    })

    it('should run the job if runNow is true (default)', () => {
      Worker.create(opts)
      assert.ok(Worker.prototype.run.calledOnce, '.run called')
    })

    it('should hold the job if runNow is not true', () => {
      const testOpts = assign({ runNow: false }, opts)
      Worker.create(testOpts)
      assert.notOk(Worker.prototype.run.calledOnce, '.run not called')
    })

    it('should default the timeout to not exist', () => {
      const w = Worker.create(opts)
      assert.equal(w.msTimeout, 0, 'set the timeout correctly')
    })

    it('should use the given logger', (done) => {
      const log = { info: noop }
      opts.log = log
      const w = Worker.create(opts)
      assert.equal(w.log, log)
      done()
    })

    it('should use the given errorCat', (done) => {
      opts.errorCat = 'mew'
      const w = Worker.create(opts)
      assert.equal(w.errorCat, 'mew')
      done()
    })

    describe('with worker timeout', () => {
      let prevTimeout

      before(() => {
        prevTimeout = process.env.WORKER_TIMEOUT
        process.env.WORKER_TIMEOUT = 4000
      })

      after(() => { process.env.WORKER_TIMEOUT = prevTimeout })

      it('should use the environment timeout', () => {
        const w = Worker.create(opts)
        assert.equal(w.msTimeout, 4 * 1000, 'set the timeout correctly')
      })

      it('should throw when given a non-integer', () => {
        opts.msTimeout = 'foobar'
        assert.throws(() => {
          Worker.create(opts)
        }, /not an integer/)
      })

      it('should throw when given a negative timeout', () => {
        opts.msTimeout = -230
        assert.throws(() => {
          Worker.create(opts)
        }, /is negative/)
      })
    })
  })

  describe('_reportError', () => {
    let worker
    const queue = 'some-queue-name'
    const job = { foo: 'barrz' }

    beforeEach(() => {
      worker = Worker.create(opts)
      sinon.stub(worker.errorCat, 'report')
      worker.queue = queue
      worker.job = job
    })

    afterEach(() => {
      worker.errorCat.report.restore()
    })

    it('should set data on the error', () => {
      const error = new Error('an error')
      worker._reportError(error)
      assert.isObject(error.data)
    })

    it('should set queue data', () => {
      const error = new Error('an error')
      worker._reportError(error)
      assert.equal(error.data.queue, queue)
    })

    it('should set job data', () => {
      const error = new Error('an error')
      worker._reportError(error)
      assert.deepEqual(error.data.job, job)
    })

    it('should not remove given data', () => {
      const error = new Error('an error')
      error.data = { custom: 'foo' }
      worker._reportError(error)
      assert.equal(error.data.custom, 'foo')
    })

    it('should report the error via error-cat', () => {
      const error = new Error('an error')
      worker._reportError(error)
      assert.ok(worker.errorCat.report.calledOnce)
      assert.ok(worker.errorCat.report.calledWith(error))
    })
  })

  describe('_eventTags', () => {
    let worker
    const queue = 'some.queue.name'

    beforeEach(() => {
      worker = Worker.create(opts)
      worker.queue = queue
    })

    it('should generate tags for new style queues', () => {
      const tags = worker._eventTags()
      assert.isObject(tags)
      assert.equal(Object.keys(tags).length, 4)
      assert.deepEqual(tags, {
        queue: queue,
        token0: 'name',
        token1: 'queue.name',
        token2: 'some.queue.name'
      })
    })

    it('should generate tags for old style queues', () => {
      const queue = 'some-queue-name'
      worker.queue = queue
      const tags = worker._eventTags()
      assert.isObject(tags)
      assert.equal(Object.keys(tags).length, 2)
      assert.deepEqual(tags, {
        queue: queue,
        token0: 'some-queue-name'
      })
    })
  })

  describe('_incMonitor', () => {
    let worker
    const queue = 'do.something.command'

    beforeEach(() => {
      sinon.stub(monitor, 'increment')
      worker = Worker.create(put({ runNow: false }, opts))
      worker.queue = queue
    })

    afterEach(() => {
      monitor.increment.restore()
    })

    it('should call monitor increment for event without result tag', () => {
      worker._incMonitor('ponos')
      sinon.assert.calledOnce(monitor.increment)
      sinon.assert.calledWith(monitor.increment, 'ponos', {
        token0: 'command',
        token1: 'something.command',
        token2: 'do.something.command',
        queue: 'do.something.command'
      })
    })

    it('should call monitor increment for event with extra tags', () => {
      worker._incMonitor('ponos.finish', { result: 'success' })
      sinon.assert.calledOnce(monitor.increment)
      sinon.assert.calledWith(monitor.increment, 'ponos.finish', {
        token0: 'command',
        token1: 'something.command',
        token2: 'do.something.command',
        queue: 'do.something.command',
        result: 'success'
      })
    })

    describe('with disabled monitoring', () => {
      beforeEach(() => {
        process.env.WORKER_MONITOR_DISABLED = 'true'
      })

      afterEach(() => {
        delete process.env.WORKER_MONITOR_DISABLED
      })

      it('should not call monitor increment', () => {
        worker._incMonitor('ponos.finish', { result: 'success' })
        sinon.assert.notCalled(monitor.increment)
      })
    })
  })

  describe('_createTimer', () => {
    let worker
    const queue = 'do.something.command'

    beforeEach(() => {
      sinon.stub(monitor, 'timer').returns({ stop: () => {} })
      worker = Worker.create(put({ runNow: false }, opts))
      worker.queue = queue
    })

    afterEach(() => {
      monitor.timer.restore()
    })

    it('should call monitor.timer for event without result tag', () => {
      const timer = worker._createTimer()
      assert.isNotNull(timer)
      assert.isNotNull(timer.stop)
      sinon.assert.calledOnce(monitor.timer)
      sinon.assert.calledWith(monitor.timer, 'ponos.timer', true, {
        token0: 'command',
        token1: 'something.command',
        token2: 'do.something.command',
        queue: 'do.something.command'
      })
    })

    describe('with disabled monitoring', () => {
      beforeEach(() => {
        process.env.WORKER_MONITOR_DISABLED = 'true'
      })

      afterEach(() => {
        delete process.env.WORKER_MONITOR_DISABLED
      })

      it('should not call monitor.timer', () => {
        const timer = worker._createTimer()
        assert.isNull(timer)
        sinon.assert.notCalled(monitor.timer)
      })
    })
  })

  describe('run', () => {
    let worker
    const timer = {
      stop: () => {}
    }

    beforeEach(() => {
      opts.runNow = false
      worker = Worker.create(opts)
      sinon.stub(monitor, 'increment')
      sinon.stub(monitor, 'timer').returns(timer)
      sinon.spy(timer, 'stop')
    })

    afterEach(() => {
      monitor.increment.restore()
      monitor.timer.restore()
      timer.stop.restore()
    })

    describe('successful runs', () => {
      it('should run the task and call done', () => {
        taskHandler = sinon.stub()
        doneHandler = sinon.stub()
        return assert.isFulfilled(worker.run())
          .then(() => {
            assert.ok(taskHandler.calledOnce, 'task was called once')
            assert.ok(doneHandler.calledOnce, 'done was called once')
            sinon.assert.calledTwice(monitor.increment)
            sinon.assert.calledWith(monitor.increment.firstCall, 'ponos', {
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledWith(monitor.increment.secondCall, 'ponos.finish', {
              result: 'success',
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledOnce(monitor.timer)
            sinon.assert.calledWith(monitor.timer, 'ponos.timer', true, {
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledOnce(timer.stop)
          })
      })

      describe('with disabled monitoring', () => {
        beforeEach(() => {
          process.env.WORKER_MONITOR_DISABLED = 'true'
        })

        afterEach(() => {
          delete process.env.WORKER_MONITOR_DISABLED
        })

        it('should run the task and call done and not stop timer', () => {
          taskHandler = sinon.stub()
          doneHandler = sinon.stub()
          return assert.isFulfilled(worker.run())
            .then(() => {
              assert.ok(taskHandler.calledOnce, 'task was called once')
              assert.ok(doneHandler.calledOnce, 'done was called once')
              sinon.assert.notCalled(monitor.increment)
              sinon.assert.notCalled(monitor.timer)
              sinon.assert.notCalled(timer.stop)
            })
        })
      })
      describe('with worker timeout', () => {
        let prevTimeout

        before(() => {
          prevTimeout = process.env.WORKER_TIMEOUT
          process.env.WORKER_TIMEOUT = 10
        })

        after(() => { process.env.WORKER_TIMEOUT = prevTimeout })

        it('should timeout the job', () => {
          // we are going to replace the handler w/ a stub
          taskHandler = () => {
            taskHandler = sinon.stub()
            return Promise.resolve().delay(25)
          }
          doneHandler = sinon.stub()
          return assert.isFulfilled(worker.run())
            .then(() => {
              // this is asserting taskHandler called once, but was twice
              assert.ok(taskHandler.calledOnce, 'task was called once')
              assert.ok(doneHandler.calledOnce, 'done was called once')
              sinon.assert.callCount(monitor.increment, 5)
              sinon.assert.calledWith(monitor.increment.firstCall, 'ponos', {
                token0: 'command',
                token1: 'something.command',
                token2: 'do.something.command',
                queue: 'do.something.command'
              })
              sinon.assert.calledWith(monitor.increment.secondCall, 'ponos.finish', {
                result: 'timeout-error',
                token0: 'command',
                token1: 'something.command',
                token2: 'do.something.command',
                queue: 'do.something.command'
              })
              sinon.assert.calledTwice(monitor.timer)
              sinon.assert.calledWith(monitor.timer, 'ponos.timer', true, {
                token0: 'command',
                token1: 'something.command',
                token2: 'do.something.command',
                queue: 'do.something.command'
              })
              sinon.assert.calledTwice(timer.stop)
            })
        })
      })

      describe('with max retry delay', () => {
        let prevDelay

        before(() => {
          prevDelay = process.env.WORKER_MAX_RETRY_DELAY
          process.env.WORKER_MAX_RETRY_DELAY = 4
        })

        after(() => { process.env.WORKER_MAX_RETRY_DELAY = prevDelay })

        it('should exponentially back off up to max', () => {
          const initDelay = worker.retryDelay
          taskHandler = sinon.stub()
          taskHandler.onFirstCall().throws(new Error('foobar'))
          taskHandler.onSecondCall().throws(new Error('foobar'))
          taskHandler.onThirdCall().throws(new Error('foobar'))
          doneHandler = sinon.stub()
          return assert.isFulfilled(worker.run())
            .then(() => {
              assert.notEqual(initDelay, worker.retryDelay, 'delay increased')
              assert.equal(worker.retryDelay, 4, 'delay increased to max')
              assert.equal(taskHandler.callCount, 4, 'task was called twice')
              assert.ok(doneHandler.calledOnce, 'done was called once')
            })
        })
      })
    })

    describe('errored behavior', () => {
      beforeEach(() => {
        doneHandler = sinon.stub()
        sinon.stub(worker.log, 'error')
        sinon.stub(worker.log, 'warn')
        sinon.stub(worker, '_reportError')
      })

      afterEach(() => {
        worker.log.error.restore()
        worker.log.warn.restore()
        worker._reportError.restore()
      })

      it('should catch TaskFatalError, not retry, and call done', () => {
        taskHandler = sinon.stub().throws(new TaskFatalError('queue', 'foobar'))
        return assert.isFulfilled(worker.run())
          .then(() => {
            assert.ok(taskHandler.calledOnce, 'task was called once')
            assert.ok(doneHandler.calledOnce, 'done was called once')
            sinon.assert.calledTwice(monitor.increment)
            sinon.assert.calledWith(monitor.increment.firstCall, 'ponos', {
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledWith(monitor.increment.secondCall, 'ponos.finish', {
              result: 'fatal-error',
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledOnce(monitor.timer)
            sinon.assert.calledWith(monitor.timer, 'ponos.timer', true, {
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledOnce(timer.stop)
          })
      })

      it('should retry if task throws Error', () => {
        taskHandler = sinon.stub()
        taskHandler.onFirstCall().throws(new Error('foobar'))
        return assert.isFulfilled(worker.run())
          .then(() => {
            assert.equal(taskHandler.callCount, 2, 'task was called twice')
            assert.ok(doneHandler.calledOnce, 'done was called once')
            sinon.assert.callCount(monitor.increment, 4)
            sinon.assert.calledWith(monitor.increment.firstCall, 'ponos', {
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledWith(monitor.increment.secondCall, 'ponos.finish', {
              result: 'task-error',
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledTwice(monitor.timer)
            sinon.assert.calledWith(monitor.timer, 'ponos.timer', true, {
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledTwice(timer.stop)
          })
      })

      it('should retry if task throws TaskError', () => {
        taskHandler = sinon.stub()
        taskHandler.onFirstCall().throws(new TaskError('foobar'))
        return assert.isFulfilled(worker.run())
          .then(() => {
            assert.equal(taskHandler.callCount, 2, 'task was called twice')
            assert.ok(doneHandler.calledOnce, 'done was called once')
            sinon.assert.callCount(monitor.increment, 4)
            sinon.assert.calledWith(monitor.increment.firstCall, 'ponos', {
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledWith(monitor.increment.secondCall, 'ponos.finish', {
              result: 'task-error',
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledTwice(monitor.timer)
            sinon.assert.calledWith(monitor.timer, 'ponos.timer', true, {
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledTwice(timer.stop)
          })
      })

      it('should retry if task throws TimeoutError', () => {
        taskHandler = sinon.stub()
        taskHandler.onFirstCall().throws(new TimeoutError())
        return assert.isFulfilled(worker.run())
          .then(() => {
            assert.equal(taskHandler.callCount, 2, 'task was called twice')
            assert.ok(doneHandler.calledOnce, 'done was called once')
            sinon.assert.callCount(monitor.increment, 5)
            sinon.assert.calledWith(monitor.increment.firstCall, 'ponos', {
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledWith(monitor.increment.secondCall, 'ponos.finish', {
              result: 'timeout-error',
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledTwice(monitor.timer)
            sinon.assert.calledWith(monitor.timer, 'ponos.timer', true, {
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledTwice(timer.stop)
          })
      })

      it('should log TaskFatalError', () => {
        const fatalError = new TaskFatalError('queue', 'foobar')
        taskHandler = sinon.stub().throws(fatalError)
        return assert.isFulfilled(worker.run())
          .then(() => {
            assert.ok(worker.log.error.calledOnce)
            assert.deepEqual(worker.log.error.firstCall.args[0], {
              err: fatalError
            })
          })
      })

      it('should report TaskFatalError', () => {
        const fatalError = new TaskFatalError('queue', 'foobar')
        taskHandler = sinon.stub().throws(fatalError)
        return assert.isFulfilled(worker.run())
          .then(() => {
            assert.ok(worker._reportError.calledWith(fatalError))
          })
      })

      it('should log all other errors', () => {
        const otherError = new Error('stfunoob')
        taskHandler = sinon.stub()
        taskHandler.onFirstCall().throws(otherError)
        return assert.isFulfilled(worker.run())
          .then(() => {
            assert.ok(worker.log.warn.calledOnce, 'log called once')
            assert.equal(worker.log.warn.firstCall.args[0].err, otherError)
            sinon.assert.calledWith(monitor.increment.firstCall, 'ponos', {
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledWith(monitor.increment.secondCall, 'ponos.finish', {
              result: 'task-error',
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledTwice(monitor.timer)
            sinon.assert.calledWith(monitor.timer, 'ponos.timer', true, {
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
            sinon.assert.calledTwice(timer.stop)
          })
      })

      it('should report all other errors', () => {
        const otherError = new Error('stfunoob')
        taskHandler = sinon.stub()
        taskHandler.onFirstCall().throws(otherError)
        return assert.isFulfilled(worker.run())
          .then(() => {
            assert.ok(worker._reportError.calledWith(otherError))
          })
      })
    })
  })
})
