'use strict'

const chai = require('chai')
const clone = require('101/clone')
const errorCat = require('error-cat')
const noop = require('101/noop')
const Promise = require('bluebird')
const sinon = require('sinon')

const logger = require('../../src/logger')
const ponos = require('../../src')
const RabbitMQ = require('../../src/rabbitmq')
const RedisRateLimiter = require('../../src/rate-limiters/redis')
const Worker = require('../../src/worker')

const assert = chai.assert

const tasks = {
  'test-queue-01': worker,
  'test-queue-02': {
    task: worker,
    msTimeout: 4242
  }
}
function worker (job, headers, done) {} // eslint-disable-line no-unused-vars

describe('Server', () => {
  let server
  let runStub
  const testJobMeta = {
    appId: 'Cedric',
    timestamp: Date.now()
  }

  beforeEach(() => {
    runStub = sinon.stub().resolves()
    sinon.stub(Worker, 'create').returns({
      run: runStub
    })
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

    it('should create redisRateLimiter if options passed', () => {
      const s = new ponos.Server({
        redisRateLimiter: {
          port: '1234',
          host: 'localhost'
        }
      })
      assert.ok(s)
      assert.instanceOf(s._redisRateLimiter, RedisRateLimiter)
    })

    it('should not have redisRateLimiter if option not passed', () => {
      const s = new ponos.Server()
      assert.ok(s)
      assert.isUndefined(s._redisRateLimiter)
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

    it('should create rabbitmq with any provided options', () => {
      const s = new ponos.Server({
        rabbitmq: { hostname: 'foobar' }
      })
      assert.equal(s._rabbitmq.hostname, 'foobar')
    })

    it('should default the name of the rabbitmq client to ponos', () => {
      const s = new ponos.Server({
        rabbitmq: { hostname: 'foobar' }
      })
      assert.equal(s._rabbitmq.name, 'ponos')
    })

    it('should should name the rabbitmq client if we named our server', () => {
      const s = new ponos.Server({
        name: 'my-awesome-ponos-server',
        rabbitmq: { hostname: 'foobar' }
      })
      assert.equal(s._rabbitmq.name, 'my-awesome-ponos-server')
    })
  })

  describe('consume', () => {
    let server
    let opts = { tasks: tasks }

    beforeEach(() => {
      server = new ponos.Server(opts)
      sinon.stub(RabbitMQ.prototype, 'consume').resolves()
    })

    afterEach(() => {
      RabbitMQ.prototype.consume.restore()
    })

    it('should start consuming', () => {
      return assert.isFulfilled(server.consume())
        .then(() => {
          sinon.assert.calledOnce(RabbitMQ.prototype.consume)
          sinon.assert.calledWithExactly(RabbitMQ.prototype.consume)
        })
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

    describe('the worker it creates', () => {
      const mockJob = { foo: 'bar' }
      const mockDone = () => {}

      beforeEach(() => {
        sinon.stub(ponos.Server.prototype, '_enqueue').returns()
        return server._subscribeAll()
      })

      afterEach(() => {
        ponos.Server.prototype._enqueue.restore()
      })

      it('should be created on subscribeToQueue', () => {
        const worker = RabbitMQ.prototype.subscribeToQueue.firstCall.args.pop()
        worker(mockJob, testJobMeta, mockDone)
        sinon.assert.calledOnce(ponos.Server.prototype._enqueue)
        sinon.assert.calledWithExactly(
          ponos.Server.prototype._enqueue,
          'test-queue-01',
          sinon.match.func,
          mockJob,
          testJobMeta,
          mockDone
        )
      })

      it('should be created on subscribeToFanoutExchange', () => {
        const worker = RabbitMQ.prototype.subscribeToFanoutExchange
          .firstCall.args.pop()
        worker(mockJob, testJobMeta, mockDone)
        sinon.assert.calledOnce(ponos.Server.prototype._enqueue)
        sinon.assert.calledWithExactly(
          ponos.Server.prototype._enqueue,
          'test-queue-01',
          sinon.match.func,
          mockJob,
          testJobMeta,
          mockDone
        )
      })
    })
  })

  describe('_enqueue', () => {
    let job
    let jobMeta
    let done
    beforeEach(() => {
      sinon.stub(server, '_workLoop').returns()
      sinon.stub(server, '_runWorker').returns()
      server._workQueues = {
        'Harry': [],
        'Potter': [Promise.resolve()]
      }
      job = {
        Ron: 'Weasley'
      }
      jobMeta = {
        appId: 'api'
      }
      done = sinon.stub()
    })

    it('should add job to work queue', () => {
      server._enqueue('Harry', Promise.resolve(), job, jobMeta, done)
      assert.equal(server._workQueues.Harry.length, 1)
    })

    it('should bind worker correctly', () => {
      server._enqueue('Harry', 'Hermione', job, jobMeta, done)
      server._workQueues.Harry[0]()
      sinon.assert.calledOnce(server._runWorker)
      sinon.assert.calledWith(server._runWorker, 'Harry', 'Hermione', job, jobMeta, done)
    })

    it('should run work loop if first item', () => {
      server._enqueue('Harry', Promise.resolve(), job, jobMeta, done)
      sinon.assert.calledOnce(server._workLoop)
      sinon.assert.calledWith(server._workLoop, 'Harry')
    })

    it('should NOT run work loop if not item', () => {
      server._enqueue('Potter', Promise.resolve(), job, done)
      sinon.assert.notCalled(server._workLoop)
    })
  }) // end _enqueue

  describe('_workLoop', () => {
    let stub1
    let stub2
    let stub3

    beforeEach(() => {
      sinon.spy(server, '_workLoop')
      sinon.stub(server.errorCat, 'report')
      stub1 = sinon.stub()
      stub2 = sinon.stub()
      stub3 = sinon.stub()
      server._workQueues = {
        Rubeus: [stub1, stub2],
        Hagrid: [stub3],
        Draco: []
      }
    })

    afterEach(() => {
      server.errorCat.report.restore()
    })

    it('should stop if nothing in queue', () => {
      return server._workLoop('Draco')
        .then(() => {
          sinon.assert.calledOnce(server._workLoop)
        })
    })

    it('should run each worker', () => {
      return server._workLoop('Rubeus')
        .then(function loop () {
          if (server._workLoop.callCount !== 3) {
            return Promise.delay(5).then(loop)
          }
        })
        .then(() => {
          sinon.assert.callCount(server._workLoop, 3)
          sinon.assert.calledOnce(stub1)
          sinon.assert.calledOnce(stub2)
          sinon.assert.notCalled(stub3)
        })
    })

    it('should call limit if it exits', () => {
      const limitStub = sinon.stub().resolves()
      server._redisRateLimiter = {
        limit: limitStub
      }
      const testOpts = {
        Hannah: 'Abbott'
      }
      const testName = 'Draco'
      server._workerOptions[testName] = testOpts
      return server._workLoop(testName)
        .then(() => {
          sinon.assert.calledOnce(limitStub)
          sinon.assert.calledWith(limitStub, testName, testOpts)
        })
    })

    it('should ignore rate limit error', () => {
      const testErr = new Error('DeathEater')
      const limitStub = sinon.stub().rejects(testErr)
      server._redisRateLimiter = {
        limit: limitStub
      }
      const testOpts = {
        Hannah: 'Abbott'
      }
      const testName = 'Hagrid'
      server._workerOptions[testName] = testOpts
      return assert.isFulfilled(server._workLoop(testName))
        .then(function loop () {
          if (server._workLoop.callCount !== 2) {
            return Promise.delay(5).then(loop)
          }
        })
        .then(() => {
          sinon.assert.calledTwice(server.errorCat.report)
          sinon.assert.calledWith(server.errorCat.report, testErr)
          sinon.assert.calledTwice(limitStub)
          sinon.assert.calledWith(limitStub, testName, testOpts)
          sinon.assert.calledOnce(stub3)
        })
    })
  }) // end _workLoop

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
      assert.isFulfilled(server._runWorker('test-queue-01', taskHandler, { bar: 'baz' }, testJobMeta, noop))
        .then(() => {
          sinon.assert.calledOnce(Worker.create)
          sinon.assert.calledWithExactly(
            Worker.create,
            sinon.match.has('queue', 'test-queue-01')
          )
        })
    })

    it('should provide the given job', () => {
      const job = { bar: 'baz' }
      assert.isFulfilled(server._runWorker('test-queue-01', taskHandler, job, testJobMeta, noop))
        .then(() => {
          sinon.assert.calledWithExactly(
            Worker.create,
            sinon.match.has('job', job)
          )
        })
    })

    it('should call done if worker rejected', () => {
      const stub = sinon.stub().rejects(new Error('Baggins'))
      assert.isFulfilled(server._runWorker('test-queue-01', taskHandler, { bar: 'baz' }, testJobMeta, stub))
        .then(() => {
          sinon.assert.calledOnce(stub)
        })
    })

    it('should call done if worker resolved', () => {
      const stub = sinon.stub().resolves(new Error('Bombadil'))
      assert.isFulfilled(server._runWorker('test-queue-01', taskHandler, { bar: 'baz' }, testJobMeta, stub))
        .then(() => {
          sinon.assert.calledOnce(stub)
        })
    })

    it('should provide the correct task handler', () => {
      assert.isFulfilled(server._runWorker('test-queue-01', taskHandler, { bar: 'baz' }, testJobMeta, noop))
        .then(() => {
          sinon.assert.calledOnce(Worker.create)
          sinon.assert.calledWithExactly(
            Worker.create,
            sinon.match.has('task', taskHandler)
          )
        })
    })

    it('should provide the correct logger', () => {
      assert.isFulfilled(server._runWorker('test-queue-01', taskHandler, { bar: 'baz' }, testJobMeta, noop))
        .then(() => {
          sinon.assert.calledOnce(Worker.create)
          sinon.assert.calledWithExactly(
            Worker.create,
            sinon.match.has('log', server.log)
          )
        })
    })

    it('should provide the correct errorCat', () => {
      assert.isFulfilled(server._runWorker('test-queue-01', taskHandler, { bar: 'baz' }, testJobMeta, noop))
        .then(() => {
          sinon.assert.calledOnce(Worker.create)
          sinon.assert.calledWithExactly(
            Worker.create,
            sinon.match.has('errorCat', server.errorCat)
          )
        })
    })

    it('should correctly provide custom worker options', () => {
      assert.isFulfilled(server._runWorker('test-queue-02', taskHandler, { bar: 'baz' }, testJobMeta, noop))
        .then(() => {
          sinon.assert.calledOnce(Worker.create)
          sinon.assert.calledWithExactly(
            Worker.create,
            sinon.match.has('msTimeout', 4242)
          )
        })
    })

    it('should call run on new worker', () => {
      assert.isFulfilled(server._runWorker('test-queue-02', taskHandler, { bar: 'baz' }, testJobMeta, noop))
        .then(() => {
          sinon.assert.calledOnce(runStub)
        })
    })
  })

  describe('setEvent', () => {
    const testQueue99 = 'test-queue-99'

    it('should set the event handler', () => {
      server.setEvent(testQueue99, worker)
      assert.isFunction(server._events.get(testQueue99))
    })

    it('should throw if the provided event is not a function', () => {
      assert.throws(() => {
        server.setEvent(testQueue99, 'not-a-function')
      }, /must be a function/)
    })

    it('should throw if the provided opts is not an object', () => {
      assert.throws(() => {
        server.setEvent(testQueue99, worker, 'not-an-object')
      }, /must be a object/)
    })

    it('should set default worker options', () => {
      server.setEvent(testQueue99, worker)
      assert.deepEqual(server._workerOptions[testQueue99], {})
    })

    it('should set worker options for events', () => {
      const opts = { msTimeout: 2000 }
      server.setEvent(testQueue99, worker, opts)
      assert.deepEqual(server._workerOptions[testQueue99], opts)
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

    it('should throw if the provided opts is not an object', () => {
      assert.throws(() => {
        server.setTask(testQueue99, worker, 'not-an-object')
      }, /must be a object/)
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
  })

  describe('setAllEvents', () => {
    beforeEach(() => {
      server = new ponos.Server()
      sinon.stub(server, 'setEvent')
    })

    afterEach(() => {
      server.setEvent.restore()
    })

    it('should throw if not set with object', () => {
      assert.throws(
        () => { server.setAllEvents(1738) },
        /called with.+object/
      )
    })

    it('should set multiple tasks', () => {
      server.setAllEvents(tasks)
      sinon.assert.callCount(server.setEvent, 2)
    })

    describe('with task objects', () => {
      beforeEach(() => {
        sinon.stub(server.log, 'warn')
      })

      afterEach(() => {
        server.log.warn.restore()
      })

      it('should log a warning if a event is not defined', () => {
        const brokenEvents = clone(tasks)
        delete brokenEvents['test-queue-02']
        brokenEvents['test-queue-01'] = { msTimeout: 2000 }
        server.setAllEvents(brokenEvents)
        sinon.assert.calledOnce(server.log.warn)
        sinon.assert.calledWith(
          server.log.warn,
          sinon.match.has('key', 'test-queue-01'),
          'no task function defined for key'
        )
        sinon.assert.notCalled(server.setEvent)
      })

      it('should pass options when calling setEvent', () => {
        const newEvents = {}
        const opts = newEvents['test-queue-99'] = {
          task: noop,
          msTimeout: 2000
        }
        server.setAllEvents(newEvents)
        assert.ok(server.setEvent.calledOnce)
        assert.deepEqual(server.setEvent.firstCall.args[2], opts)
      })
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

    it('should throw if not set with object', () => {
      assert.throws(
        () => { server.setAllTasks(1738) },
        /called with.+object/
      )
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
      sinon.stub(RedisRateLimiter.prototype, 'connect').resolves()
      sinon.stub(ponos.Server.prototype, '_subscribeAll').resolves()
      sinon.stub(ponos.Server.prototype, 'consume').resolves()
      sinon.stub(errorCat, 'report').returns()
    })

    afterEach(() => {
      RabbitMQ.prototype.connect.restore()
      RedisRateLimiter.prototype.connect.restore()
      ponos.Server.prototype._subscribeAll.restore()
      ponos.Server.prototype.consume.restore()
      errorCat.report.restore()
    })

    it('should connect to rabbitmq', () => {
      return assert.isFulfilled(server.start())
        .then(() => {
          sinon.assert.calledOnce(RabbitMQ.prototype.connect)
        })
    })

    it('should call connect on redis', () => {
      server._redisRateLimiter = new RedisRateLimiter({
        host: 'localhost',
        port: '4242',
        log: logger
      })
      return assert.isFulfilled(server.start())
        .then(() => {
          sinon.assert.calledOnce(RedisRateLimiter.prototype.connect)
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
          sinon.assert.calledOnce(errorCat.report)
          sinon.assert.calledWithExactly(errorCat.report, error)
        })
    })
  })

  describe('stop', () => {
    let server

    beforeEach(() => {
      server = new ponos.Server({ tasks: tasks })
      sinon.stub(RabbitMQ.prototype, 'unsubscribe').resolves()
      sinon.stub(RabbitMQ.prototype, 'disconnect').resolves()
      sinon.stub(errorCat, 'report')
    })

    afterEach(() => {
      RabbitMQ.prototype.unsubscribe.restore()
      RabbitMQ.prototype.disconnect.restore()
      errorCat.report.restore()
    })

    it('should unsubscribe the rabbitmq connection', () => {
      return assert.isFulfilled(server.stop())
        .then(() => {
          sinon.assert.calledOnce(RabbitMQ.prototype.unsubscribe)
        })
    })

    it('should close the rabbitmq connection', () => {
      return assert.isFulfilled(server.stop())
        .then(() => {
          sinon.assert.calledOnce(RabbitMQ.prototype.disconnect)
        })
    })

    it('should report and rethrow stop errors', () => {
      const unsubError = new Error('Hermes is tired...')
      RabbitMQ.prototype.unsubscribe.rejects(unsubError)
      return assert.isRejected(server.stop())
        .then(() => {
          sinon.assert.calledOnce(errorCat.report)
          sinon.assert.calledWithExactly(errorCat.report, unsubError)
        })
    })
  })
})
