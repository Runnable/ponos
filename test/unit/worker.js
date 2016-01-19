'use strict'

var chai = require('chai')
var assert = chai.assert
var sinon = require('sinon')

var Promise = require('bluebird')
var monitor = require('monitor-dog')
var TaskError = require('../../lib/errors/task-error')
var TaskFatalError = require('../../lib/errors/task-fatal-error')
var TimeoutError = Promise.TimeoutError
var Worker = require('../../lib/worker')
var assign = require('101/assign')
var noop = require('101/noop')
var put = require('101/put')

describe('Worker', function () {
  var opts
  var taskHandler
  var doneHandler

  beforeEach(function () {
    opts = {
      queue: 'do.something.command',
      task: function (data) { return Promise.resolve(data).then(taskHandler) },
      job: { message: 'hello world' },
      done: function () { return Promise.resolve().then(doneHandler) }
    }
  })

  describe('Constructor', function () {
    beforeEach(function () { sinon.stub(Worker.prototype, 'run') })

    afterEach(function () { Worker.prototype.run.restore() })

    it('should enforce default opts', function () {
      var testOpts = assign({}, opts)
      testOpts.job = null
      assert.throws(function () {
        Worker.create(testOpts)
      }, /job is required.+Worker/)
    })

    it('should run the job if runNow is true (default)', function () {
      Worker.create(opts)
      assert.ok(Worker.prototype.run.calledOnce, '.run called')
    })

    it('should hold the job if runNow is not true', function () {
      var testOpts = assign({ runNow: false }, opts)
      Worker.create(testOpts)
      assert.notOk(Worker.prototype.run.calledOnce, '.run not called')
    })

    it('should default the timeout to not exist', function () {
      var w = Worker.create(opts)
      assert.equal(w.msTimeout, 0, 'set the timeout correctly')
    })

    it('should use the given logger', function (done) {
      var log = { info: noop }
      opts.log = log
      var w = Worker.create(opts)
      assert.equal(w.log, log)
      done()
    })

    it('should use the given errorCat', function (done) {
      opts.errorCat = 'mew'
      var w = Worker.create(opts)
      assert.equal(w.errorCat, 'mew')
      done()
    })

    describe('with worker timeout', function () {
      var prevTimeout

      before(function () {
        prevTimeout = process.env.WORKER_TIMEOUT
        process.env.WORKER_TIMEOUT = 4000
      })

      after(function () { process.env.WORKER_TIMEOUT = prevTimeout })

      it('should use the environment timeout', function () {
        var w = Worker.create(opts)
        assert.equal(w.msTimeout, 4 * 1000, 'set the timeout correctly')
      })

      it('should throw when given a non-integer', function () {
        opts.msTimeout = 'foobar'
        assert.throws(function () {
          Worker.create(opts)
        }, /not an integer/)
      })

      it('should throw when given a negative timeout', function () {
        opts.msTimeout = -230
        assert.throws(function () {
          Worker.create(opts)
        }, /is negative/)
      })
    })
  })

  describe('_reportError', function () {
    var worker
    var queue = 'some-queue-name'
    var job = { foo: 'barrz' }

    beforeEach(function () {
      worker = Worker.create(opts)
      sinon.stub(worker.errorCat, 'report')
      worker.queue = queue
      worker.job = job
    })

    afterEach(function () {
      worker.errorCat.report.restore()
    })

    it('should set data on the error', function () {
      var error = new Error('an error')
      worker._reportError(error)
      assert.isObject(error.data)
    })

    it('should set queue data', function () {
      var error = new Error('an error')
      worker._reportError(error)
      assert.equal(error.data.queue, queue)
    })

    it('should set job data', function () {
      var error = new Error('an error')
      worker._reportError(error)
      assert.deepEqual(error.data.job, job)
    })

    it('should not remove given data', function () {
      var error = new Error('an error')
      error.data = { custom: 'foo' }
      worker._reportError(error)
      assert.equal(error.data.custom, 'foo')
    })

    it('should report the error via error-cat', function () {
      var error = new Error('an error')
      worker._reportError(error)
      assert.ok(worker.errorCat.report.calledOnce)
      assert.ok(worker.errorCat.report.calledWith(error))
    })
  })

  describe('_eventTags', function () {
    var worker
    var queue = 'some.queue.name'

    beforeEach(function () {
      worker = Worker.create(opts)
      worker.queue = queue
    })
    it('should generate tags for new style queues', function () {
      var tags = worker._eventTags()
      assert.isObject(tags)
      assert.equal(Object.keys(tags).length, 4)
      assert.deepEqual(tags, {
        queue: queue,
        token0: 'name',
        token1: 'queue.name',
        token2: 'some.queue.name'
      })
    })
    it('should generate tags for old style queues', function () {
      var queue = 'some-queue-name'
      worker.queue = queue
      var tags = worker._eventTags()
      assert.isObject(tags)
      assert.equal(Object.keys(tags).length, 2)
      assert.deepEqual(tags, {
        queue: queue,
        token0: 'some-queue-name'
      })
    })
  })

  describe('_inc', function () {
    var worker
    var queue = 'do.something.command'

    beforeEach(function () {
      sinon.stub(monitor, 'increment')
      worker = Worker.create(put({ runNow: false }, opts))
      worker.queue = queue
    })
    afterEach(function () {
      monitor.increment.restore()
    })
    it('should call monitor increment for event without result tag', function () {
      worker._inc('ponos')
      sinon.assert.calledOnce(monitor.increment)
      sinon.assert.calledWith(monitor.increment, 'ponos', {
        token0: 'command',
        token1: 'something.command',
        token2: 'do.something.command',
        queue: 'do.something.command'
      })
    })
    it('should call monitor increment for event with extra tags', function () {
      worker._inc('ponos.finish', { result: 'success' })
      sinon.assert.calledOnce(monitor.increment)
      sinon.assert.calledWith(monitor.increment, 'ponos.finish', {
        token0: 'command',
        token1: 'something.command',
        token2: 'do.something.command',
        queue: 'do.something.command',
        result: 'success'
      })
    })
    describe('with disabled monitoring', function () {
      beforeEach(function () {
        process.env.WORKER_MONITOR_DISABLED = 'true'
      })
      afterEach(function () {
        delete process.env.WORKER_MONITOR_DISABLED
      })
      it('should not call monitor increment', function () {
        worker._inc('ponos.finish', { result: 'success' })
        sinon.assert.notCalled(monitor.increment)
      })
    })
  })

  describe('run', function () {
    var worker
    var timer = {
      stop: function () {}
    }

    beforeEach(function () {
      opts.runNow = false
      worker = Worker.create(opts)
      sinon.stub(monitor, 'increment')
      sinon.stub(monitor, 'timer').returns(timer)
      sinon.spy(timer, 'stop')
    })

    afterEach(function () {
      monitor.increment.restore()
      monitor.timer.restore()
      timer.stop.restore()
    })

    describe('successful runs', function () {
      it('should run the task and call done', function () {
        taskHandler = sinon.stub()
        doneHandler = sinon.stub()
        return assert.isFulfilled(worker.run())
          .then(function () {
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
            sinon.assert.calledWith(monitor.timer, 'ponos.timer', {
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
          })
      })
      describe('with worker timeout', function () {
        var prevTimeout

        before(function () {
          prevTimeout = process.env.WORKER_TIMEOUT
          process.env.WORKER_TIMEOUT = 10
        })

        after(function () { process.env.WORKER_TIMEOUT = prevTimeout })

        it('should timeout the job', function () {
          // we are going to replace the handler w/ a stub
          taskHandler = function () {
            taskHandler = sinon.stub()
            return Promise.resolve().delay(25)
          }
          doneHandler = sinon.stub()
          return assert.isFulfilled(worker.run())
            .then(function () {
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
              sinon.assert.calledWith(monitor.timer, 'ponos.timer', {
                token0: 'command',
                token1: 'something.command',
                token2: 'do.something.command',
                queue: 'do.something.command'
              })
            })
        })
      })

      describe('with max retry delay', function () {
        var prevDelay

        before(function () {
          prevDelay = process.env.WORKER_MAX_RETRY_DELAY
          process.env.WORKER_MAX_RETRY_DELAY = 4
        })

        after(function () { process.env.WORKER_MAX_RETRY_DELAY = prevDelay })

        it('should exponentially back off up to max', function () {
          var initDelay = worker.retryDelay
          taskHandler = sinon.stub()
          taskHandler.onFirstCall().throws(new Error('foobar'))
          taskHandler.onSecondCall().throws(new Error('foobar'))
          taskHandler.onThirdCall().throws(new Error('foobar'))
          doneHandler = sinon.stub()
          return assert.isFulfilled(worker.run())
            .then(function () {
              assert.notEqual(initDelay, worker.retryDelay, 'delay increased')
              assert.equal(worker.retryDelay, 4, 'delay increased to max')
              assert.equal(taskHandler.callCount, 4, 'task was called twice')
              assert.ok(doneHandler.calledOnce, 'done was called once')
            })
        })
      })
    })

    describe('errored behavior', function () {
      beforeEach(function () {
        doneHandler = sinon.stub()
        sinon.stub(worker.log, 'error')
        sinon.stub(worker.log, 'warn')
        sinon.stub(worker, '_reportError')
      })

      afterEach(function () {
        worker.log.error.restore()
        worker.log.warn.restore()
        worker._reportError.restore()
      })

      it('should catch TaskFatalError, not retry, and call done', function () {
        taskHandler = sinon.stub().throws(new TaskFatalError('foobar'))
        return assert.isFulfilled(worker.run())
          .then(function () {
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
            sinon.assert.calledWith(monitor.timer, 'ponos.timer', {
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
          })
      })

      it('should retry if task throws Error', function () {
        taskHandler = sinon.stub()
        taskHandler.onFirstCall().throws(new Error('foobar'))
        return assert.isFulfilled(worker.run())
          .then(function () {
            assert.equal(taskHandler.callCount, 2, 'task was called twice')
            assert.ok(doneHandler.calledOnce, 'done was called once')
          })
      })

      it('should retry if task throws TaskError', function () {
        taskHandler = sinon.stub()
        taskHandler.onFirstCall().throws(new TaskError('foobar'))
        return assert.isFulfilled(worker.run())
          .then(function () {
            assert.equal(taskHandler.callCount, 2, 'task was called twice')
            assert.ok(doneHandler.calledOnce, 'done was called once')
          })
      })

      it('should retry if task throws TimeoutError', function () {
        taskHandler = sinon.stub()
        taskHandler.onFirstCall().throws(new TimeoutError())
        return assert.isFulfilled(worker.run())
          .then(function () {
            assert.equal(taskHandler.callCount, 2, 'task was called twice')
            assert.ok(doneHandler.calledOnce, 'done was called once')
          })
      })

      it('should log TaskFatalError', function () {
        var fatalError = new TaskFatalError('foobar')
        taskHandler = sinon.stub().throws(fatalError)
        return assert.isFulfilled(worker.run())
          .then(function () {
            assert.ok(worker.log.error.calledOnce)
            assert.deepEqual(worker.log.error.firstCall.args[0], {
              err: fatalError
            })
          })
      })

      it('should report TaskFatalError', function () {
        var fatalError = new TaskFatalError('foobar')
        taskHandler = sinon.stub().throws(fatalError)
        return assert.isFulfilled(worker.run())
          .then(function () {
            assert.ok(worker._reportError.calledWith(fatalError))
          })
      })

      it('should log all other errors', function () {
        var otherError = new Error('stfunoob')
        taskHandler = sinon.stub()
        taskHandler.onFirstCall().throws(otherError)
        return assert.isFulfilled(worker.run())
          .then(function () {
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
            sinon.assert.calledWith(monitor.timer, 'ponos.timer', {
              token0: 'command',
              token1: 'something.command',
              token2: 'do.something.command',
              queue: 'do.something.command'
            })
          })
      })

      it('should report all other errors', function () {
        var otherError = new Error('stfunoob')
        taskHandler = sinon.stub()
        taskHandler.onFirstCall().throws(otherError)
        return assert.isFulfilled(worker.run())
          .then(function () {
            assert.ok(worker._reportError.calledWith(otherError))
          })
      })
    })
  })
})
