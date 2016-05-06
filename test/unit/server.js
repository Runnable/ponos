'use strict'

const chai = require('chai')
const clone = require('101/clone')
const noop = require('101/noop')
const Promise = require('bluebird')
const sinon = require('sinon')

const assert = chai.assert

const ponos = require('../../')
const Worker = require('../../lib/worker')
const RabbitMQ = require('../../lib/rabbitmq')

const tasks = {
  'test-queue-01': worker,
  'test-queue-02': {
    task: worker,
    msTimeout: 4242
  }
}
function worker (job, done) {} // eslint-disable-line no-unused-vars

describe('Server', () => {
  let server

  beforeEach(() => {
    sinon.stub(Worker, 'create')
    server = new ponos.Server({ tasks: tasks })
  })

  afterEach(() => {
    Worker.create.restore()
  })

  describe('Constructor', () => {
    beforeEach(() => {
      sinon.stub(ponos.Server.prototype, 'setAllTasks').returns()
      sinon.stub(ponos.Server.prototype, 'setAllEvents').returns()
    })

    afterEach(() => {
      ponos.Server.prototype.setAllTasks.restore()
      ponos.Server.prototype.setAllEvents.restore()
    })

    it('should create a server w/o any options', () => {
      const s = new ponos.Server()
      assert.ok(s)
    })

    it('should use the default logger', () => {
      const s = new ponos.Server()
      assert.equal(s.log.fields.module, 'ponos:server')
    })

    it('should set tasks if they were provided', () => {
      const opts = {
        tasks: tasks
      }
      const s = new ponos.Server(opts)
      assert.ok(s)
      sinon.assert.notCalled(ponos.Server.prototype.setAllEvents)
      sinon.assert.calledOnce(ponos.Server.prototype.setAllTasks)
      sinon.assert.calledWithExactly(
        ponos.Server.prototype.setAllTasks,
        tasks
      )
    })

    it('should not call setAllTasks if they were not provided', () => {
      const s = new ponos.Server()
      assert.ok(s)
      sinon.assert.notCalled(ponos.Server.prototype.setAllTasks)
    })

    it('should set events if they were provided', () => {
      const opts = {
        events: tasks
      }
      const s = new ponos.Server(opts)
      assert.ok(s)
      sinon.assert.calledOnce(ponos.Server.prototype.setAllEvents)
      sinon.assert.calledWithExactly(
        ponos.Server.prototype.setAllEvents,
        tasks
      )
    })

    it('should not call setAllEvents if they were not provided', () => {
      const s = new ponos.Server()
      assert.ok(s)
      sinon.assert.notCalled(ponos.Server.prototype.setAllEvents)
    })

    it('should use the provided logger', () => {
      const customLogger = {}
      const s = new ponos.Server({
        log: customLogger
      })
      assert.equal(s.log, customLogger)
    })
  })

  describe('_subscribeAll', () => {
    let server
    let opts = {
      tasks: tasks,
      events: tasks
    }

    beforeEach(() => {
      server = new ponos.Server(opts)
      sinon.stub(RabbitMQ.prototype, 'subscribeToFanoutExchange').resolves()
      sinon.stub(RabbitMQ.prototype, 'subscribeToQueue').resolves()
    })

    afterEach(() => {
      RabbitMQ.prototype.subscribeToFanoutExchange.restore()
      RabbitMQ.prototype.subscribeToQueue.restore()
    })

    it('should subscribe to all task queues using RabbitMQ', () => {
      return assert.isFulfilled(server._subscribeAll())
        .then(() => {
          sinon.assert.calledTwice(RabbitMQ.prototype.subscribeToQueue)
          sinon.assert.calledWithExactly(
            RabbitMQ.prototype.subscribeToQueue,
            'test-queue-01',
            sinon.match.func
          )
          sinon.assert.calledWithExactly(
            RabbitMQ.prototype.subscribeToQueue,
            'test-queue-02',
            sinon.match.func
          )
        })
    })

    it('should subscribe to all event queues using RabbitMQ', () => {
      return assert.isFulfilled(server._subscribeAll())
        .then(() => {
          sinon.assert.calledTwice(RabbitMQ.prototype.subscribeToFanoutExchange)
          sinon.assert.calledWithExactly(
            RabbitMQ.prototype.subscribeToFanoutExchange,
            'test-queue-01',
            sinon.match.func
          )
          sinon.assert.calledWithExactly(
            RabbitMQ.prototype.subscribeToFanoutExchange,
            'test-queue-02',
            sinon.match.func
          )
        })
    })
  })

  describe.skip('_unsubscribe', () => {
    let server
    let opts = {
      tasks: tasks,
      events: tasks
    }

    beforeEach(() => {
      server = new ponos.Server(opts)
    })

    it('should apply the correct callback for the given queue', () => {
      return assert.isFulfilled(server._unsubscribe('a', null))
        .then(() => {
          assert.ok(server.hermes.unsubscribe.calledOnce)
          assert.ok(server.hermes.unsubscribe.calledWith('a'))
        })
    })
  })

  describe.skip('_unsubscribeAll', () => {
    let server
    let opts = {
      tasks: tasks,
      events: tasks
    }

    beforeEach(() => {
      server = new ponos.Server(opts)
    })

    it.skip('should call `_unsubscribe` for each queue', () => {
      return assert.isFulfilled(server._unsubscribeAll())
        .then(() => {
          assert.ok(server._unsubscribe.calledTwice)
          assert.ok(server._unsubscribe.calledWith('a'))
          assert.ok(server._unsubscribe.calledWith('b'))
        })
    })
  })

  describe('_runWorker', () => {
    let server
    let opts = {
      tasks: tasks,
      events: tasks
    }
    const taskHandler = () => { return 'yuss' }

    beforeEach(() => {
      server = new ponos.Server(opts)
    })

    it('should provide the correct queue name', () => {
      server._runWorker('test-queue-01', taskHandler, { bar: 'baz' }, noop)
      sinon.assert.calledOnce(Worker.create)
      sinon.assert.calledWithExactly(
        Worker.create,
        sinon.match.has('queue', 'test-queue-01')
      )
    })

    it('should provide the given job', () => {
      const job = { bar: 'baz' }
      server._runWorker('test-queue-01', taskHandler, job, noop)
      sinon.assert.calledWithExactly(
        Worker.create,
        sinon.match.has('job', job)
      )
    })

    it('should provide the given done function', () => {
      server._runWorker('test-queue-01', taskHandler, { bar: 'baz' }, noop)
      sinon.assert.calledOnce(Worker.create)
      sinon.assert.calledWithExactly(
        Worker.create,
        sinon.match.has('done', noop)
      )
    })

    it('should provide the correct task handler', () => {
      server._runWorker('test-queue-01', taskHandler, { bar: 'baz' }, noop)
      sinon.assert.calledOnce(Worker.create)
      sinon.assert.calledWithExactly(
        Worker.create,
        sinon.match.has('task', taskHandler)
      )
    })

    it('should provide the correct logger', () => {
      server._runWorker('test-queue-01', taskHandler, { bar: 'baz' }, noop)
      sinon.assert.calledOnce(Worker.create)
      sinon.assert.calledWithExactly(
        Worker.create,
        sinon.match.has('log', server.log)
      )
    })

    it('should provide the correct errorCat', () => {
      server._runWorker('test-queue-01', taskHandler, { bar: 'baz' }, noop)
      sinon.assert.calledOnce(Worker.create)
      sinon.assert.calledWithExactly(
        Worker.create,
        sinon.match.has('errorCat', server.errorCat)
      )
    })

    it('should correctly provide custom worker options', () => {
      server._runWorker('test-queue-02', taskHandler, { bar: 'baz' }, noop)
      sinon.assert.calledOnce(Worker.create)
      sinon.assert.calledWithExactly(
        Worker.create,
        sinon.match.has('msTimeout', 4242)
      )
    })
  })

  describe('setTask', () => {
    const testQueue99 = 'test-queue-99'

    it('should set the task handler', () => {
      server.setTask(testQueue99, worker)
      assert.isFunction(server._tasks.get(testQueue99))
    })

    it('should throw if the provided task is not a function', () => {
      assert.throws(() => {
        server.setTask(testQueue99, 'not-a-function')
      }, /must be a function/)
    })

    it('should set default worker options', () => {
      server.setTask(testQueue99, worker)
      assert.deepEqual(server._workerOptions[testQueue99], {})
    })

    it('should set worker options for tasks', () => {
      const opts = { msTimeout: 2000 }
      server.setTask(testQueue99, worker, opts)
      assert.deepEqual(server._workerOptions[testQueue99], opts)
    })

    it('should pick only provided options that are valid', () => {
      const opts = { msTimeout: 2000, foo: 'bar', not: 'athing' }
      const expectedOpts = { msTimeout: 2000 }
      server.setTask(testQueue99, worker, opts)
      assert.deepEqual(server._workerOptions[testQueue99], expectedOpts)
    })
  })

  describe('setAllTasks', () => {
    beforeEach(() => {
      server = new ponos.Server()
      sinon.stub(server, 'setTask')
    })

    afterEach(() => {
      server.setTask.restore()
    })

    it('should set multiple tasks', () => {
      server.setAllTasks(tasks)
      sinon.assert.callCount(server.setTask, 2)
    })

    describe('with task objects', () => {
      beforeEach(() => {
        sinon.stub(server.log, 'warn')
      })

      afterEach(() => {
        server.log.warn.restore()
      })

      it('should log a warning if a task is not defined', () => {
        const brokenTasks = clone(tasks)
        delete brokenTasks['test-queue-02']
        brokenTasks['test-queue-01'] = { msTimeout: 2000 }
        server.setAllTasks(brokenTasks)
        sinon.assert.calledOnce(server.log.warn)
        sinon.assert.calledWith(
          server.log.warn,
          sinon.match.has('key', 'test-queue-01'),
          'no task function defined for key'
        )
        sinon.assert.notCalled(server.setTask)
      })

      it('should pass options when calling setTask', () => {
        const newTasks = {}
        const opts = newTasks['test-queue-99'] = { task: noop, msTimeout: 2000 }
        server.setAllTasks(newTasks)
        assert.ok(server.setTask.calledOnce)
        assert.deepEqual(server.setTask.firstCall.args[2], opts)
      })
    })
  })

  describe('start', () => {
    beforeEach(() => {
      sinon.stub(RabbitMQ.prototype, 'connect').resolves()
      sinon.stub(ponos.Server.prototype, '_subscribeAll').resolves()
      sinon.stub(ponos.Server.prototype, 'consume').resolves()
      sinon.stub(server.errorCat, 'report').returns()
    })

    afterEach(() => {
      RabbitMQ.prototype.connect.restore()
      ponos.Server.prototype._subscribeAll.restore()
      ponos.Server.prototype.consume.restore()
      server.errorCat.report.restore()
    })

    it('should connect to rabbitmq', () => {
      return assert.isFulfilled(server.start())
        .then(() => {
          sinon.assert.calledOnce(RabbitMQ.prototype.connect)
        })
    })

    it('should report rabbit errors', () => {
      const error = new Error('nope error')
      RabbitMQ.prototype.connect.rejects(error)
      return assert.isRejected(
        server.start(),
        /nope error/
      )
        .then(() => {
          sinon.assert.calledOnce(server.errorCat.report)
          sinon.assert.calledWithExactly(server.errorCat.report, error)
        })
    })
  })

  describe.skip('stop', () => {
    it('should close the hermes connection', () => {
      return assert.isFulfilled(server.stop())
        .then(() => {
          assert.ok(server.hermes.closeAsync.calledOnce)
        })
    })

    it('should report and rethrow stop errors', () => {
      const closeError = new Error('Hermes is tired...')
      server.hermes.closeAsync.returns(Promise.reject(closeError))
      return assert.isRejected(server.stop())
        .then(() => {
          assert.ok(server.errorCat.report.calledWith(closeError))
        })
    })
  })
})
