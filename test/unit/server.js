'use strict';

var chai = require('chai');
var assert = chai.assert;
var sinon = require('sinon');

var Promise = require('bluebird');
var hermes = require('runnable-hermes');
var noop = require('101/noop');

var ponos = require('../../');
var Worker = require('../../lib/worker');
var ponosDefaultLogger = require('../../lib/logger');

var tasks = {
  'test-queue-01': worker,
  'test-queue-02': worker
};
function worker (job, done) {}; // eslint-disable-line no-unused-vars

describe('Server', function () {
  var server;
  beforeEach(function () { sinon.stub(Worker, 'create'); });
  beforeEach(function () {
    server = new ponos.Server({ queues: Object.keys(tasks) });
    sinon.stub(server.hermes, 'connectAsync').returns(Promise.resolve());
    sinon.stub(server.hermes, 'subscribe').returns(Promise.resolve());
    sinon.stub(server.hermes, 'closeAsync').returns(Promise.resolve());
    sinon.spy(server.errorCat, 'report');
  });
  afterEach(function () {
    server.hermes.connectAsync.restore();
    server.hermes.subscribe.restore();
    server.hermes.closeAsync.restore();
    server.errorCat.report.restore();
  });
  afterEach(function () { Worker.create.restore(); });

  describe('Constructor', function () {
    beforeEach(function () {
      sinon.stub(hermes, 'hermesSingletonFactory').returns(noop);
    });
    afterEach(function () { hermes.hermesSingletonFactory.restore(); });

    describe('with rabbitmq env vars', function () {
      var envs = {
        HOSTNAME: 'foobar',
        PORT: 42,
        USERNAME: 'luke',
        PASSWORD: 'skywalker'
      };

      before(function () {
        Object.keys(envs).forEach(function (k) {
          var oldVal = process.env['RABBITMQ_' + k];
          process.env['RABBITMQ_' + k] = envs[k];
          envs[k] = oldVal;
        });
      });

      after(function () {
        Object.keys(envs).forEach(function (k) {
          process.env['RABBITMQ_' + k] = envs[k];
        });
      });

      it('should create a hermes client with those options', function () {
        var s = new ponos.Server({ queues: Object.keys(tasks) });
        assert.ok(s);
        var hermesOpts = hermes.hermesSingletonFactory.firstCall.args[0];
        assert.deepEqual(hermesOpts, {
          hostname: 'foobar',
          port: 42,
          username: 'luke',
          password: 'skywalker',
          queues: [ 'test-queue-01', 'test-queue-02' ]
        });
      });
    });

    it('should take a Hermes client', function () {
      var s = new ponos.Server({ hermes: noop });
      assert.ok(s);
      assert.equal(s.hermes, noop);
    });

    it('should default rabbit mq vars', function () {
      var s = new ponos.Server({ queues: Object.keys(tasks) });
      assert.ok(s);
      assert.equal(s.hermes, noop);
      var hermesOpts = hermes.hermesSingletonFactory.firstCall.args[0];
      assert.deepEqual(hermesOpts, {
        hostname: 'localhost',
        port: 5672,
        username: 'guest',
        password: 'guest',
        queues: [ 'test-queue-01', 'test-queue-02' ]
      });
    });

    it('should require a list of queues', function () {
      assert.throws(function () {
        new ponos.Server();
      }, /missing.+queues/);
    });

    it('should require a string list of queues', function () {
      assert.throws(function () {
        new ponos.Server({ queues: [{}] });
      }, /queues.+string/);
    });

    it('should use the default logger', function () {
      var s = new ponos.Server({ queues: Object.keys(tasks) });
      assert.equal(s.log, ponosDefaultLogger);
    });

    it('should use the provided logger', function () {
      var customLogger = {};
      var s = new ponos.Server({
        queues: Object.keys(tasks),
        log: customLogger
      });
      assert.equal(s.log, customLogger);
    });
  });

  describe('_assertHaveAllTasks', function () {
    afterEach(function () { hermes.hermesSingletonFactory.restore(); });

    it('should reject when a queue is missing a task handler', function () {
      // runnable-hermes 6.1.0 introduced .getQueues
      sinon.stub(hermes, 'hermesSingletonFactory').returns({
        getQueues: sinon.stub().returns([ 'a', 'b' ])
      });
      var s = new ponos.Server({ queues: [ 'a', 'b' ] });
      s.setTask('b', noop);
      assert.isRejected(s._assertHaveAllTasks(), /handler not defined/);
    });

    it('should accept when all queues have task handlers', function () {
      // runnable-hermes 6.1.0 introduced .getQueues
      sinon.stub(hermes, 'hermesSingletonFactory').returns({
        getQueues: sinon.stub().returns(['a'])
      });
      var s = new ponos.Server({ queues: ['a'] });
      s.setTask('a', noop);
      assert.isFulfilled(s._assertHaveAllTasks());
    });
  });

  describe('_subscribe', function () {
    var server;
    var queues = ['a'];

    beforeEach(function () {
      // runnable-hermes 6.1.0 introduced .getQueues
      sinon.stub(hermes, 'hermesSingletonFactory').returns({
        subscribe: noop,
        getQueues: sinon.stub().returns(queues)
      });
      server = new ponos.Server({ queues: queues });
      server.setAllTasks({ a: noop });
      sinon.stub(server.hermes, 'subscribe');
      sinon.stub(server, '_runWorker');
    });

    afterEach(function () {
      server.hermes.subscribe.restore();
      hermes.hermesSingletonFactory.restore();
    });

    it('should apply the correct callback for the given queue', function () {
      server._subscribe('a');
      assert.ok(server.hermes.subscribe.calledOnce);
      assert.ok(server.hermes.subscribe.calledWith('a'));
      var hermesCallback = server.hermes.subscribe.firstCall.args[1];
      var job = { foo: 'bar' };
      hermesCallback(job, noop);
      assert.ok(server._runWorker.calledWith('a', job, noop));
    });
  });

  describe('_subscribeAll', function () {
    var server;
    var queues = [ 'a', 'b' ];

    beforeEach(function () {
      // runnable-hermes 6.1.0 introduced .getQueues
      sinon.stub(hermes, 'hermesSingletonFactory').returns({
        subscribe: noop,
        getQueues: sinon.stub().returns(queues)
      });
      server = new ponos.Server({ queues: queues });
      sinon.stub(server, '_subscribe');
      return server.setAllTasks({ a: noop, b: noop });
    });

    afterEach(function () { hermes.hermesSingletonFactory.restore(); });

    it('should call `_subscribe` for each queue', function () {
      return assert.isFulfilled(server._subscribeAll())
        .then(function () {
          assert.ok(server._subscribe.calledTwice);
          assert.ok(server._subscribe.calledWith('a'));
          assert.ok(server._subscribe.calledWith('b'));
        });
    });
  });

  describe('_runWorker', function () {
    var server;
    var taskHandler = function () { return 'yuss'; };
    var msTimeout = 1932848;

    beforeEach(function () {
      sinon.stub(hermes, 'hermesSingletonFactory').returns({ subscribe: noop });
      server = new ponos.Server({ queues: ['a'] });
      server.setTask('a', taskHandler, { msTimeout: msTimeout });
    });

    afterEach(function () {
      hermes.hermesSingletonFactory.restore();
    });

    it('should provide the correct queue name', function () {
      server._runWorker('a', { bar: 'baz' }, noop);
      assert.ok(Worker.create.calledOnce);
      var opts = Worker.create.firstCall.args[0];
      assert.equal(opts.queue, 'a');
    });

    it('should provide the given job', function () {
      var job = { bar: 'baz' };
      server._runWorker('a', job, noop);
      assert.ok(Worker.create.calledOnce);
      var opts = Worker.create.firstCall.args[0];
      assert.equal(opts.job, job);
    });

    it('should provide the given done function', function () {
      server._runWorker('a', { bar: 'baz' }, noop);
      assert.ok(Worker.create.calledOnce);
      var opts = Worker.create.firstCall.args[0];
      assert.equal(opts.done, noop);
    });

    it('should provide the correct task handler', function () {
      server._runWorker('a', { bar: 'baz' }, noop);
      assert.ok(Worker.create.calledOnce);
      var opts = Worker.create.firstCall.args[0];
      assert.equal(opts.task, taskHandler);
    });

    it('should provide the correct logger', function () {
      server._runWorker('a', { bar: 'baz' }, noop);
      assert.ok(Worker.create.calledOnce);
      var opts = Worker.create.firstCall.args[0];
      assert.equal(opts.log, server.log);
    });

    it('should provide the correct errorCat', function () {
      server._runWorker('a', { bar: 'baz' }, noop);
      assert.ok(Worker.create.calledOnce);
      var opts = Worker.create.firstCall.args[0];
      assert.equal(opts.errorCat, server.errorCat);
    });

    it('should correctly provide custom worker options', function () {
      server._runWorker('a', { bar: 'baz' }, noop);
      assert.ok(Worker.create.calledOnce);
      var opts = Worker.create.firstCall.args[0];
      assert.equal(opts.msTimeout, msTimeout);
    });
  });

  describe('setTask', function () {
    var queue = Object.keys(tasks)[0];
    it('should set the task handler', function () {
      server.setTask(queue, worker);
      assert.isFunction(server._tasks[queue]);
    });

    it('should throw if the provided task is not a function', function () {
      assert.throws(function () {
        server.setTask(queue, 'not-a-function');
      }, /not a function/);
    });

    it('should set default worker options', function () {
      server.setTask(queue, worker);
      assert.deepEqual(server._workerOptions[queue], {});
    });

    it('should set worker options for tasks', function () {
      var opts = { msTimeout: 2000 };
      server.setTask(queue, worker, opts);
      assert.deepEqual(server._workerOptions[queue], opts);
    });

    it('should pick only provided options that are valid', function () {
      var opts = { msTimeout: 2000, foo: 'bar', not: 'athing' };
      var expectedOpts = { msTimeout: 2000 };
      server.setTask(queue, worker, opts);
      assert.deepEqual(server._workerOptions[queue], expectedOpts);
    });
  });

  describe('setAllTasks', function () {
    var queue = Object.keys(tasks)[0];

    beforeEach(function () {
      sinon.spy(server, 'setTask');
    });

    it('should set multiple tasks', function () {
      var numTasks = Object.keys(tasks).length;
      server.setAllTasks(tasks);
      assert.equal(server.setTask.callCount, numTasks);
      assert.equal(Object.keys(server._tasks).length, numTasks);
    });

    describe('with option objects', function () {
      it('should throw if a task is not defined', function () {
        var tasks = {};
        tasks[queue] = { msTimeout: 2000 };
        assert.throws(function () {
          server.setAllTasks(tasks);
        }, /No task function defined/);
        assert.equal(server.setTask.callCount, 0);
      });

      it('should pass options when calling setTask', function () {
        var tasks = {};
        var opts = tasks[queue] = { task: noop, msTimeout: 2000 };
        server.setAllTasks(tasks);
        assert.ok(server.setTask.calledOnce);
        assert.deepEqual(server.setTask.firstCall.args[2], opts);
      });
    });
  });

  describe('start', function () {
    describe('without tasks', function () {
      it('should fail', function () {
        return assert.isRejected(server.start(), /handler not defined/);
      });
    });

    describe('with tasks', function () {
      beforeEach(function () { return server.setAllTasks(tasks); });

      it('should subscribe to all queues', function () {
        return assert.isFulfilled(server.start())
          .then(function () {
            assert.equal(server.hermes.subscribe.callCount, 2);
          });
      });

      it('should enqueue a function that creates workers', function () {
        return assert.isFulfilled(server.start())
          .then(function () {
            assert.equal(server.hermes.subscribe.callCount, 2);
            // get the function that was placed for the queue
            var fn = server.hermes.subscribe.getCall(0).args.pop();
            assert.isFunction(fn);
            assert.equal(Worker.create.callCount, 0);
            // call the function that was enqueued
            fn({}, noop);
            assert.equal(Worker.create.callCount, 1);
            var opts = Worker.create.getCall(0).args.pop();
            assert.deepEqual(opts, {
              queue: 'test-queue-01',
              job: {},
              task: worker,
              done: noop,
              log: server.log,
              errorCat: server.errorCat
            });
          });
      });

      it('should report start errors', function () {
        var startError = new Error('I don\'t want to...');
        server.hermes.connectAsync.returns(Promise.reject(startError));
        assert.isRejected(server.start())
          .then(function () {
            assert.ok(server.errorCat.report.calledWith(startError));
          });
      });
    });
  });

  describe('stop', function () {
    it('should close the hermes connection', function () {
      return assert.isFulfilled(server.stop())
        .then(function () {
          assert.ok(server.hermes.closeAsync.calledOnce);
        });
    });

    it('should report and rethrow stop errors', function () {
      var closeError = new Error('Hermes is tired...');
      server.hermes.closeAsync.returns(Promise.reject(closeError));
      return assert.isRejected(server.stop())
        .then(function () {
          assert.ok(server.errorCat.report.calledWith(closeError));
        });
    });
  });
});
