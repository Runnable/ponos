'use strict';

var chai = require('chai');
chai.use(require('chai-as-promised'));
var assert = chai.assert;
var sinon = require('sinon');

var Promise = require('bluebird');
var Worker = require('../../lib/worker');
var hermes = require('runnable-hermes');
var noop = require('101/noop');
var ponos = require('../../');

var tasks = {
  'test-queue-01': worker,
  'test-queue-02': worker
};
function resolvePromise () { return Promise.resolve(); }
function worker (job, done) {}; // eslint-disable-line no-unused-vars

describe('Server', function () {
  var server;
  before(function () { sinon.stub(Worker, 'create'); });
  beforeEach(function () {
    server = new ponos.Server({ queues: Object.keys(tasks) });
    sinon.stub(server.hermes, 'connectAsync', resolvePromise);
    sinon.stub(server.hermes, 'subscribeAsync', resolvePromise);
  });
  afterEach(function () {
    server.hermes.connectAsync.restore();
    server.hermes.subscribeAsync.restore();
  });
  after(function () { Worker.create.restore(); });

  describe('Constructor', function () {
    beforeEach(function () {
      sinon.stub(hermes, 'hermesSingletonFactory').returns(noop);
    });
    afterEach(function () { hermes.hermesSingletonFactory.restore(); });

    it('should take a Hermes client', function () {
      var s = new ponos.Server({ hermes: noop });
      assert.ok(s);
      assert.equal(s.hermes, noop);
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
  });

  describe('setTask', function () {
    it('should accept tasks, and not subscribe', function () {
      var queue = Object.keys(tasks)[0];
      return assert.isFulfilled(server.setTask(queue, worker))
        .then(function () {
          assert.notOk(server.hermes.subscribeAsync.calledOnce);
          assert.isFunction(server._tasks[queue]);
        });
    });
  });

  describe('setAllTasks', function () {
    it('should set multiple tasks', function () {
      sinon.spy(server, 'setTask');
      var numTasks = Object.keys(tasks).length;
      return assert.isFulfilled(server.setAllTasks(tasks))
        .then(function () {
          assert.equal(server.setTask.callCount, numTasks);
          assert.equal(Object.keys(server._tasks).length, numTasks);
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
              done: noop
            });
          });
      });
    });
  });

  describe('stop', function () {
    it('should close the hermes connection', function () {
      sinon.stub(server.hermes, 'closeAsync', resolvePromise);
      return assert.isFulfilled(server.stop())
        .then(function () {
          assert.ok(server.hermes.closeAsync.calledOnce);
        });
    });
  });
});
