'use strict'

const bunyan = require('bunyan')
const chai = require('chai')
const hermes = require('runnable-hermes')
const noop = require('101/noop')
const Promise = require('bluebird')
const sinon = require('sinon')

const assert = chai.assert

const ponos = require('../../')
const Worker = require('../../lib/worker')

const tasks = {
  'test-queue-01': worker,
  'test-queue-02': worker
}
function worker (job, done) {} // eslint-disable-line no-unused-vars

describe('Server', () => {
  let server

  beforeEach(() => { sinon.stub(Worker, 'create') })

  beforeEach(() => {
    server = new ponos.Server({ queues: Object.keys(tasks) })
    sinon.stub(server.hermes, 'connectAsync').returns(Promise.resolve())
    sinon.stub(server.hermes, 'subscribe').returns(Promise.resolve())
    sinon.stub(server.hermes, 'closeAsync').returns(Promise.resolve())
    sinon.spy(server.errorCat, 'report')
  })

  afterEach(() => {
    server.hermes.connectAsync.restore()
    server.hermes.subscribe.restore()
    server.hermes.closeAsync.restore()
    server.errorCat.report.restore()
  })

  afterEach(() => { Worker.create.restore() })

  describe('Constructor', () => {
    beforeEach(() => {
      sinon.stub(hermes, 'hermesSingletonFactory').returns(noop)
    })

    afterEach(() => { hermes.hermesSingletonFactory.restore() })

    describe('with rabbitmq env vars', () => {
      let envs = {
        HOSTNAME: 'foobar',
        PORT: 42,
        USERNAME: 'luke',
        PASSWORD: 'skywalker'
      }

      before(() => {
        Object.keys(envs).forEach((k) => {
          const oldVal = process.env['RABBITMQ_' + k]
          process.env['RABBITMQ_' + k] = envs[k]
          envs[k] = oldVal
        })
      })

      after(() => {
        Object.keys(envs).forEach((k) => {
          process.env['RABBITMQ_' + k] = envs[k]
        })
      })

      it('should create a hermes client with those options', () => {
        const s = new ponos.Server({ queues: Object.keys(tasks), name: 'phonos' })
        assert.ok(s)
        const hermesOpts = hermes.hermesSingletonFactory.firstCall.args[0]
        assert.deepEqual(hermesOpts, {
          hostname: 'foobar',
          port: 42,
          username: 'luke',
          password: 'skywalker',
          queues: [ 'test-queue-01', 'test-queue-02' ],
          name: 'phonos'
        })
      })
    })

    it('should take a Hermes client', () => {
      const s = new ponos.Server({ hermes: noop })
      assert.ok(s)
      assert.equal(s.hermes, noop)
    })

    it('should default rabbit mq vars', () => {
      const s = new ponos.Server({ queues: Object.keys(tasks) })
      assert.ok(s)
      assert.equal(s.hermes, noop)
      const hermesOpts = hermes.hermesSingletonFactory.firstCall.args[0]
      assert.deepEqual(hermesOpts, {
        hostname: 'localhost',
        port: 5672,
        username: 'guest',
        password: 'guest',
        queues: [ 'test-queue-01', 'test-queue-02' ],
        name: 'ponos'
      })
    })

    it('should require a list of queues', () => {
      assert.throws(() => {
        return new ponos.Server()
      }, /missing.+queues/)
    })

    it('should require a string list of queues', () => {
      assert.throws(() => {
        return new ponos.Server({ queues: [{}] })
      }, /queues.+string/)
    })

    it('should use the default logger', () => {
      const s = new ponos.Server({ queues: Object.keys(tasks) })
      assert.equal(s.log.fields.module, 'ponos:server')
    })

    it('should use the provided logger', () => {
      const customLogger = {}
      const s = new ponos.Server({
        queues: Object.keys(tasks),
        log: customLogger
      })
      assert.equal(s.log, customLogger)
    })
  })

  describe('_subscribe', () => {
    let server
    const queues = ['a']

    beforeEach(() => {
      // runnable-hermes 6.1.0 introduced .getQueues
      sinon.stub(hermes, 'hermesSingletonFactory').returns({
        subscribe: noop,
        getQueues: sinon.stub().returns(queues)
      })
      server = new ponos.Server({ queues: queues })
      server.setAllTasks({ a: noop })
      sinon.stub(server.hermes, 'subscribe')
      sinon.stub(server, '_runWorker')
      sinon.spy(bunyan.prototype, 'warn')
    })

    afterEach(() => {
      server.hermes.subscribe.restore()
      hermes.hermesSingletonFactory.restore()
      bunyan.prototype.warn.restore()
    })

    it('should apply the correct callback for the given queue', () => {
      server._subscribe('a')
      assert.ok(server.hermes.subscribe.calledOnce)
      assert.ok(server.hermes.subscribe.calledWith('a'))
      const hermesCallback = server.hermes.subscribe.firstCall.args[1]
      const job = { foo: 'bar' }
      hermesCallback(job, noop)
      assert.ok(server._runWorker.calledWith('a', job, noop))
    })

    it('should log warning if queue does not have handler', () => {
      server._subscribe('x')
      sinon.assert.notCalled(server.hermes.subscribe)
      sinon.assert.calledOnce(bunyan.prototype.warn)
      sinon.assert.calledWith(
        bunyan.prototype.warn,
        sinon.match.has('queueName', 'x'),
        'handler not defined'
      )
    })
  })

  describe('_subscribeAll', () => {
    let server
    const queues = [ 'a', 'b' ]

    beforeEach(() => {
      // runnable-hermes 6.1.0 introduced .getQueues
      sinon.stub(hermes, 'hermesSingletonFactory').returns({
        subscribe: noop,
        getQueues: sinon.stub().returns(queues)
      })
      server = new ponos.Server({ queues: queues })
      sinon.stub(server, '_subscribe')
      return server.setAllTasks({ a: noop, b: noop })
    })

    afterEach(() => { hermes.hermesSingletonFactory.restore() })

    it('should call `_subscribe` for each queue', () => {
      return assert.isFulfilled(server._subscribeAll())
        .then(() => {
          assert.ok(server._subscribe.calledTwice)
          assert.ok(server._subscribe.calledWith('a'))
          assert.ok(server._subscribe.calledWith('b'))
        })
    })
  })

  describe('_unsubscribe', () => {
    let server
    const queues = ['a']

    beforeEach(() => {
      // runnable-hermes 6.1.0 introduced .getQueues
      sinon.stub(hermes, 'hermesSingletonFactory').returns({
        unsubscribe: sinon.stub().yieldsAsync(null),
        getQueues: sinon.stub().returns(queues)
      })
      server = new ponos.Server({ queues: queues })
      server.setAllTasks({ a: noop })
    })

    afterEach(() => {
      hermes.hermesSingletonFactory.restore()
    })

    it('should apply the correct callback for the given queue', () => {
      return assert.isFulfilled(server._unsubscribe('a', null))
        .then(() => {
          assert.ok(server.hermes.unsubscribe.calledOnce)
          assert.ok(server.hermes.unsubscribe.calledWith('a'))
        })
    })
  })

  describe('_unsubscribeAll', () => {
    let server
    const queues = [ 'a', 'b' ]

    beforeEach(() => {
      // runnable-hermes 6.1.0 introduced .getQueues
      sinon.stub(hermes, 'hermesSingletonFactory').returns({
        subscribe: noop,
        unsubscribe: noop,
        getQueues: sinon.stub().returns(queues)
      })
      server = new ponos.Server({ queues: queues })
      sinon.stub(server, '_unsubscribe')
      return server.setAllTasks({ a: noop, b: noop })
    })

    afterEach(() => { hermes.hermesSingletonFactory.restore() })

    it('should call `_unsubscribe` for each queue', () => {
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
    const taskHandler = () => { return 'yuss' }
    const msTimeout = 1932848

    beforeEach(() => {
      sinon.stub(hermes, 'hermesSingletonFactory').returns({ subscribe: noop })
      server = new ponos.Server({ queues: ['a'] })
      server.setTask('a', taskHandler, { msTimeout: msTimeout })
    })

    afterEach(() => {
      hermes.hermesSingletonFactory.restore()
    })

    it('should provide the correct queue name', () => {
      server._runWorker('a', { bar: 'baz' }, noop)
      assert.ok(Worker.create.calledOnce)
      const opts = Worker.create.firstCall.args[0]
      assert.equal(opts.queue, 'a')
    })

    it('should provide the given job', () => {
      const job = { bar: 'baz' }
      server._runWorker('a', job, noop)
      assert.ok(Worker.create.calledOnce)
      const opts = Worker.create.firstCall.args[0]
      assert.equal(opts.job, job)
    })

    it('should provide the given done function', () => {
      server._runWorker('a', { bar: 'baz' }, noop)
      assert.ok(Worker.create.calledOnce)
      const opts = Worker.create.firstCall.args[0]
      assert.equal(opts.done, noop)
    })

    it('should provide the correct task handler', () => {
      server._runWorker('a', { bar: 'baz' }, noop)
      assert.ok(Worker.create.calledOnce)
      const opts = Worker.create.firstCall.args[0]
      assert.equal(opts.task, taskHandler)
    })

    it('should provide the correct logger', () => {
      server._runWorker('a', { bar: 'baz' }, noop)
      assert.ok(Worker.create.calledOnce)
      const opts = Worker.create.firstCall.args[0]
      assert.equal(opts.log, server.log)
    })

    it('should provide the correct errorCat', () => {
      server._runWorker('a', { bar: 'baz' }, noop)
      assert.ok(Worker.create.calledOnce)
      const opts = Worker.create.firstCall.args[0]
      assert.equal(opts.errorCat, server.errorCat)
    })

    it('should correctly provide custom worker options', () => {
      server._runWorker('a', { bar: 'baz' }, noop)
      assert.ok(Worker.create.calledOnce)
      const opts = Worker.create.firstCall.args[0]
      assert.equal(opts.msTimeout, msTimeout)
    })
  })

  describe('setTask', () => {
    const queue = Object.keys(tasks)[0]

    it('should set the task handler', () => {
      server.setTask(queue, worker)
      assert.isFunction(server._tasks[queue])
    })

    it('should throw if the provided task is not a function', () => {
      assert.throws(() => {
        server.setTask(queue, 'not-a-function')
      }, /must be a function/)
    })

    it('should set default worker options', () => {
      server.setTask(queue, worker)
      assert.deepEqual(server._workerOptions[queue], {})
    })

    it('should set worker options for tasks', () => {
      const opts = { msTimeout: 2000 }
      server.setTask(queue, worker, opts)
      assert.deepEqual(server._workerOptions[queue], opts)
    })

    it('should pick only provided options that are valid', () => {
      const opts = { msTimeout: 2000, foo: 'bar', not: 'athing' }
      const expectedOpts = { msTimeout: 2000 }
      server.setTask(queue, worker, opts)
      assert.deepEqual(server._workerOptions[queue], expectedOpts)
    })
  })

  describe('setAllTasks', () => {
    const queue = Object.keys(tasks)[0]

    beforeEach(() => {
      sinon.stub(server, 'setTask')
    })

    afterEach(() => {
      server.setTask.restore()
    })

    it('should set multiple tasks', () => {
      const numTasks = Object.keys(tasks).length
      server.setAllTasks(tasks)
      sinon.assert.callCount(server.setTask, numTasks)
    })

    describe('with option objects', () => {
      beforeEach(() => {
        sinon.stub(server.log, 'warn')
      })

      afterEach(() => {
        server.log.warn.restore()
      })

      it('should log a warning if a task is not defined', () => {
        let tasks = {}
        tasks[queue] = { msTimeout: 2000 }
        server.setAllTasks(tasks)
        sinon.assert.calledOnce(server.log.warn)
        sinon.assert.calledWith(
          server.log.warn,
          sinon.match.has('key', queue),
          'no task function defined for key'
        )
        sinon.assert.notCalled(server.setTask)
      })

      it('should pass options when calling setTask', () => {
        let tasks = {}
        const opts = tasks[queue] = { task: noop, msTimeout: 2000 }
        server.setAllTasks(tasks)
        assert.ok(server.setTask.calledOnce)
        assert.deepEqual(server.setTask.firstCall.args[2], opts)
      })
    })
  })

  describe('start', () => {
    describe('without tasks', () => {
      beforeEach(() => {
        sinon.stub(bunyan.prototype, 'warn')
      })

      afterEach(() => {
        bunyan.prototype.warn.restore()
      })

      it('should warn', () => {
        return server.start().then(() => {
          sinon.assert.calledTwice(bunyan.prototype.warn)
          sinon.assert.calledWith(
            bunyan.prototype.warn.firstCall,
            sinon.match.has('queueName', 'test-queue-01'),
            'handler not defined'
          )
          sinon.assert.calledWith(
            bunyan.prototype.warn.secondCall,
            sinon.match.has('queueName', 'test-queue-02'),
            'handler not defined'
          )
        })
      })
    })

    describe('with tasks', () => {
      beforeEach(() => { return server.setAllTasks(tasks) })

      it('should subscribe to all queues', () => {
        return assert.isFulfilled(server.start())
          .then(() => {
            assert.equal(server.hermes.subscribe.callCount, 2)
          })
      })

      it('should enqueue a function that creates workers', () => {
        return assert.isFulfilled(server.start())
          .then(() => {
            assert.equal(server.hermes.subscribe.callCount, 2)
            // get the function that was placed for the queue
            const fn = server.hermes.subscribe.getCall(0).args.pop()
            assert.isFunction(fn)
            assert.equal(Worker.create.callCount, 0)
            // call the function that was enqueued
            fn({}, noop)
            assert.equal(Worker.create.callCount, 1)
            const opts = Worker.create.getCall(0).args.pop()
            assert.deepEqual(opts, {
              queue: 'test-queue-01',
              job: {},
              task: worker,
              done: noop,
              log: server.log,
              errorCat: server.errorCat
            })
          })
      })

      it('should report start errors', () => {
        const startError = new Error("I don't want to...")
        server.hermes.connectAsync.returns(Promise.reject(startError))
        assert.isRejected(server.start())
          .then(() => {
            assert.ok(server.errorCat.report.calledWith(startError))
          })
      })
    })
  })

  describe('stop', () => {
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
