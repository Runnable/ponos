'use strict'

const assign = require('101/assign')
const bunyan = require('bunyan')
const chai = require('chai')
const monitor = require('monitor-dog')
const noop = require('101/noop')
const omit = require('101/omit')
const Promise = require('bluebird')
const put = require('101/put')
const sinon = require('sinon')
const WorkerError = require('error-cat/errors/worker-error')
const WorkerStopError = require('error-cat/errors/worker-stop-error')
const assert = chai.assert
const TimeoutError = Promise.TimeoutError

const Worker = require('../../src/worker')
const logger = require('../../src/logger')

describe('Worker', () => {
  let opts
  let taskHandler
  let doneHandler
  beforeEach(() => {
    opts = {
      queue: 'do.something.command',
      task: (data) => { return Promise.resolve(data).then(taskHandler) },
      job: { message: 'hello world' },
      log: logger.child({module: 'ponos:test'}),
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

    it('should enforce default opts', () => {
      const testOpts = omit(opts, 'done')
      assert.throws(() => {
        Worker.create(testOpts)
      }, /done is required.+Worker/)
    })

    it('should enforce default opts', () => {
      const testOpts = omit(opts, 'queue')
      assert.throws(() => {
        Worker.create(testOpts)
      }, /queue is required.+Worker/)
    })

    it('should enforce default opts', () => {
      const testOpts = omit(opts, 'task')
      assert.throws(() => {
        Worker.create(testOpts)
      }, /task is required.+Worker/)
    })

    it('should enforce default opts', () => {
      const testOpts = omit(opts, 'log')
      assert.throws(() => {
        Worker.create(testOpts)
      }, /log is required.+Worker/)
    })

    it('should run the job if runNow is true (default)', () => {
      Worker.create(opts)
      sinon.assert.calledOnce(Worker.prototype.run)
    })

    it('should hold the job if runNow is not true', () => {
      const testOpts = assign({ runNow: false }, opts)
      Worker.create(testOpts)
      sinon.assert.notCalled(Worker.prototype.run)
    })

    it('should default the timeout to not exist', () => {
      const w = Worker.create(opts)
      assert.equal(w.msTimeout, 0, 'set the timeout correctly')
    })

    it('should use the given logger', () => {
      const testLogger = {
        info: noop
      }
      const log = {
        child: () => { return testLogger }
      }
      opts.log = log
      const w = Worker.create(opts)
      assert.equal(w.log, testLogger)
    })

    it('should use the given errorCat', () => {
      opts.errorCat = 'mew'
      const w = Worker.create(opts)
      assert.equal(w.errorCat, 'mew')
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

    it('should report the error via error-cat', () => {
      const error = new Error('an error')
      worker._reportError(error)
      sinon.assert.calledOnce(worker.errorCat.report)
      sinon.assert.calledWithExactly(
        worker.errorCat.report,
        error
      )
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
            sinon.assert.calledOnce(taskHandler)
            sinon.assert.calledOnce(doneHandler)
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
              sinon.assert.calledOnce(taskHandler)
              sinon.assert.calledOnce(doneHandler)
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
              sinon.assert.calledOnce(taskHandler)
              sinon.assert.calledOnce(doneHandler)
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
              assert.equal(worker.retryDelay, 4)
              sinon.assert.callCount(taskHandler, 4)
              sinon.assert.calledOnce(doneHandler)
            })
        })
      })
    })

    describe('errored behavior', () => {
      beforeEach(() => {
        doneHandler = sinon.stub()
        sinon.spy(bunyan.prototype, 'error')
        sinon.spy(bunyan.prototype, 'warn')
        sinon.stub(worker, '_reportError')
      })

      afterEach(() => {
        bunyan.prototype.error.restore()
        bunyan.prototype.warn.restore()
        worker._reportError.restore()
      })

      it('should catch WorkerStopError, not retry, and call done', () => {
        taskHandler = sinon.stub().throws(new WorkerStopError('foobar'))
        return assert.isFulfilled(worker.run())
          .then(() => {
            sinon.assert.calledOnce(taskHandler)
            sinon.assert.calledOnce(doneHandler)
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
            assert.equal(taskHandler.callCount, 2)
            sinon.assert.calledOnce(doneHandler)
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
        taskHandler.onFirstCall().throws(new WorkerError('foobar'))
        return assert.isFulfilled(worker.run())
          .then(() => {
            assert.equal(taskHandler.callCount, 2)
            sinon.assert.calledOnce(doneHandler)
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
            sinon.assert.callCount(taskHandler, 2)
            sinon.assert.calledOnce(doneHandler)
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

      it('should log WorkerStopError', () => {
        const fatalError = new WorkerStopError('foobar')
        taskHandler = sinon.stub().throws(fatalError)
        return assert.isFulfilled(worker.run())
          .then(() => {
            sinon.assert.calledOnce(worker.log.error)
            sinon.assert.calledWith(
              worker.log.error.firstCall,
              { err: fatalError }
            )
          })
      })

      it('should report WorkerStopError', () => {
        const fatalError = new WorkerStopError('foobar')
        taskHandler = sinon.stub().throws(fatalError)
        return assert.isFulfilled(worker.run())
          .then(() => {
            sinon.assert.calledWithExactly(
              worker._reportError,
              fatalError
            )
          })
      })

      describe('decorateError', () => {
        it('should decorate errors with a data object', () => {
          const normalError = new Error('robot')
          taskHandler = sinon.stub()
          taskHandler.onFirstCall().throws(normalError)
          return assert.isFulfilled(worker.run())
            .then(() => {
              const decoratedError = worker._reportError.firstCall.args[0]
              assert.isObject(decoratedError.data)
            })
        })

        it('should decorate errors with the queue name', () => {
          const normalError = new Error('robot')
          taskHandler = sinon.stub()
          taskHandler.onFirstCall().throws(normalError)
          return assert.isFulfilled(worker.run())
            .then(() => {
              const decoratedError = worker._reportError.firstCall.args[0]
              assert.equal(decoratedError.data.queue, 'do.something.command')
            })
        })

        it('should decorate errors with the job', () => {
          const normalError = new Error('robot')
          taskHandler = sinon.stub()
          taskHandler.onFirstCall().throws(normalError)
          return assert.isFulfilled(worker.run())
            .then(() => {
              const decoratedError = worker._reportError.firstCall.args[0]
              assert.deepEqual(
                decoratedError.data.job,
                { message: 'hello world' }
              )
            })
        })

        it('should leave a WorkerStopError alone (already has data)', () => {
          const stopError = new WorkerStopError(
            'my message',
            { dog: 'robot' },
            { level: 'info' },
            'some.queue',
            { foo: 'bar' }
          )
          taskHandler = sinon.stub().throws(stopError)
          return assert.isFulfilled(worker.run())
            .then(() => {
              const decoratedError = worker._reportError.firstCall.args[0]
              assert.deepEqual(decoratedError, stopError)
              assert.deepEqual(decoratedError.reporting, { level: 'info' })
              assert.deepEqual(decoratedError.data.queue, 'some.queue')
              assert.deepEqual(decoratedError.data.job, { foo: 'bar' })
            })
        })
      })

      it('should log all other errors', () => {
        const otherError = new Error('stfunoob')
        taskHandler = sinon.stub()
        taskHandler.onFirstCall().throws(otherError)
        return assert.isFulfilled(worker.run())
          .then(() => {
            sinon.assert.calledOnce(worker.log.warn)
            sinon.assert.calledWith(
              worker.log.warn.firstCall,
              sinon.match.has('err', otherError)
            )
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
            sinon.assert.calledWith(
              worker._reportError,
              otherError
            )
          })
      })
    })
  })
})
