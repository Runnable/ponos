'use strict';

var chai = require('chai');
chai.use(require('chai-as-promised'));
var assert = chai.assert;
var sinon = require('sinon');

var Promise = require('bluebird');
var hermes = require('runnable-hermes');
var noop = require('101/noop');

var ponos = require('../../');
var Worker = require('../../lib/worker');
var log = require('../../lib/logger');

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
    sinon.stub(server.hermes, 'subscribeAsync').returns(Promise.resolve());
    sinon.stub(server.hermes, 'closeAsync').returns(Promise.resolve());
    sinon.spy(server.errorCat, 'report');
  });
  afterEach(function () {
    server.hermes.connectAsync.restore();
    server.hermes.subscribeAsync.restore();
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
      assert.equal(s.log, log);
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
      sinon.stub(hermes, 'hermesSingletonFactory').returns({
        queues: [ 'a', 'b' ]
      });
      var s = new ponos.Server({ queues: [ 'a', 'b' ] });
      s.setTask('b', noop)
        .then(function () {
          assert.isRejected(s._assertHaveAllTasks());
        });
    });

    it('should accept when all queues have task handlers', function () {
      sinon.stub(hermes, 'hermesSingletonFactory').returns({ queues: ['a'] });
      var s = new ponos.Server({ queues: ['a'] });
      s.setTask('a', noop)
        .then(function () {
          assert.isFulfilled(s._assertHaveAllTasks());
        });
    });
  });

  describe('_subscribe', function () {
    var server;
    var queues = ['a'];

    beforeEach(function () {
      sinon.stub(hermes, 'hermesSingletonFactory').returns({
        subscribe: noop,
        queues: queues
      });
      server = new ponos.Server({ queues: queues });
      server.setAllTasks({ a: noop });
      sinon.stub(server, '_runWorker');
      sinon.stub(server.hermes, 'subscribeAsync').returns(true);
    });

    afterEach(function () {
      hermes.hermesSingletonFactory.restore();
    });

    it('should apply the correct callback for the given queue', function () {
      server._subscribe('a');
      assert.ok(server.hermes.subscribeAsync.calledOnce);
      assert.ok(server.hermes.subscribeAsync.calledWith('a'));
      var hermesCallback = server.hermes.subscribeAsync.firstCall.args[1];
      var job = { foo: 'bar' };
      hermesCallback(job, noop);
      assert.ok(server._runWorker.calledWith('a', job, noop));
    });
  });

  describe('_subscribeAll', function () {
    var server;
    var queues = [ 'a', 'b' ];

    beforeEach(function () {
      sinon.stub(hermes, 'hermesSingletonFactory').returns({
        subscribe: noop,
        queues: queues
      });
      server = new ponos.Server({ queues: queues });
      sinon.stub(server, '_subscribe');
      return server.setAllTasks({ a: noop, b: noop });
    });

    afterEach(function () { hermes.hermesSingletonFactory.restore(); });

    it('should call `_subscribe` for each queue', function (done) {
      server._subscribeAll()
        .then(function () {
          assert.ok(server._subscribe.calledTwice);
          assert.ok(server._subscribe.calledWith('a'));
          assert.ok(server._subscribe.calledWith('b'));
          done();
        })
        .catch(done);
    });
  });

  describe('_runWorker', function () {
    var server;
    var taskHandler = function () { return 'yuss'; };
    var msTimeout = 1932848;

    beforeEach(function () {
      sinon.stub(hermes, 'hermesSingletonFactory').returns({ subscribe: noop });
      server = new ponos.Server({ queues: ['a'] });
      return server.setTask('a', taskHandler, { msTimeout: msTimeout });
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
    it('should accept tasks, and not subscribe', function () {
      return assert.isFulfilled(server.setTask(queue, worker))
        .then(function () {
          assert.notOk(server.hermes.subscribeAsync.calledOnce);
          assert.isFunction(server._tasks[queue]);
        });
    });

    it('should reject if the provided task is not a function', function () {
      return assert.isRejected(server.setTask(queue, 'not-a-function'));
    });

    it('should set default worker options', function () {
      return assert.isFulfilled(server.setTask(queue, worker))
        .then(function () {
          assert.deepEqual(server._workerOptions[queue], {});
        });
    });

    it('should set worker options for tasks', function () {
      var opts = { msTimeout: 2000 };
      return assert.isFulfilled(server.setTask(queue, worker, opts))
        .then(function () {
          assert.deepEqual(server._workerOptions[queue], opts);
        });
    });

    it('should pick only provided options that are valid', function () {
      var opts = { msTimeout: 2000, foo: 'bar', not: 'athing' };
      var expectedOpts = { msTimeout: 2000 };
      return assert.isFulfilled(server.setTask(queue, worker, opts))
        .then(function () {
          assert.deepEqual(server._workerOptions[queue], expectedOpts);
        });
    });
  });

  describe('setAllTasks', function () {
    var queue = Object.keys(tasks)[0];

    beforeEach(function () {
      sinon.spy(server, 'setTask');
    });

    it('should set multiple tasks', function () {
      var numTasks = Object.keys(tasks).length;
      return assert.isFulfilled(server.setAllTasks(tasks))
        .then(function () {
          assert.equal(server.setTask.callCount, numTasks);
          assert.equal(Object.keys(server._tasks).length, numTasks);
        });
    });

    describe('with option objects', function () {
      it('should reject if a task is not defined', function () {
        var tasks = {};
        tasks[queue] = { msTimeout: 2000 };
        return assert.isRejected(server.setAllTasks(tasks))
          .then(function () {
            assert.equal(server.setTask.callCount, 0);
          });
      });

      it('should pass options when calling setTask', function () {
        var tasks = {};
        var opts = tasks[queue] = { task: noop, msTimeout: 2000 };
        return assert.isFulfilled(server.setAllTasks(tasks))
          .then(function () {
            assert.ok(server.setTask.calledOnce);
            assert.deepEqual(server.setTask.firstCall.args[2], opts);
          });
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
            assert.equal(server.hermes.subscribeAsync.callCount, 2);
          });
      });

      it('should enqueue a function that creates workers', function () {
        return assert.isFulfilled(server.start())
          .then(function () {
            assert.equal(server.hermes.subscribeAsync.callCount, 2);
            // get the function that was placed for the queue
            var fn = server.hermes.subscribeAsync.getCall(0).args.pop();
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
