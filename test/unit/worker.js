'use strict';

var chai = require('chai');
chai.use(require('chai-as-promised'));
var assert = chai.assert;
var sinon = require('sinon');

var TaskError = require('../../lib/errors/task-error');
var TaskFatalError = require('../../lib/errors/task-fatal-error');
var TaskMaxRetriesError = require('../../lib/errors/task-max-retries-error');
var Promise = require('bluebird');
var Worker = require('../../lib/worker');
var assign = require('101/assign');
var error = require('../../lib/error');

describe('Worker', function () {
  var opts;
  var taskHandler;
  var doneHandler;
  beforeEach(function () {
    opts = {
      queue: 'a-queue',
      task: function (data) { return Promise.resolve(data).then(taskHandler); },
      job: { message: 'hello world' },
      done: function () { return Promise.resolve().then(doneHandler); }
    };
  });

  describe('Constructor', function () {
    beforeEach(function () { sinon.stub(Worker.prototype, 'run'); });
    afterEach(function () { Worker.prototype.run.restore(); });

    it('should enforce default opts', function () {
      var testOpts = assign({}, opts);
      testOpts.job = null;
      assert.throws(function () {
        Worker.create(testOpts);
      }, /job is required.+Worker/);
    });
    it('should run the job if runNow is true (default)', function () {
      Worker.create(opts);
      assert.ok(Worker.prototype.run.calledOnce);
    });
    it('should hold the job if runNow is not true', function () {
      var testOpts = assign({ runNow: false }, opts);
      Worker.create(testOpts);
      assert.notOk(Worker.prototype.run.calledOnce);
    });
  });

  describe('run', function () {
    var worker;
    beforeEach(function () {
      opts.runNow = false;
      worker = Worker.create(opts);
      sinon.stub(error, 'log');
    });
    afterEach(function () { error.log.restore(); });

    describe('successful runs', function () {
      it('should run the task and call done', function () {
        taskHandler = sinon.stub();
        doneHandler = sinon.stub();
        return assert.isFulfilled(worker.run())
          .then(function () {
            assert.ok(taskHandler.calledOnce, 'task was called once');
            assert.ok(doneHandler.calledOnce, 'done was called once');
          });
      });

      describe('with max retry delay', function () {
        var prevDelay;
        before(function () {
          prevDelay = process.env.WORKER_MAX_RETRY_DELAY;
          process.env.WORKER_MAX_RETRY_DELAY = 4;
        });
        after(function () { process.env.WORKER_MAX_RETRY_DELAY = prevDelay; });

        it('should exponentially back off', function () {
          var initDelay = worker.retryDelay;
          taskHandler = sinon.stub();
          taskHandler.onFirstCall().throws(new Error('foobar'));
          doneHandler = sinon.stub();
          return assert.isFulfilled(worker.run())
            .then(function () {
              assert.notEqual(initDelay, worker.retryDelay, 'delay increased');
              assert.equal(taskHandler.callCount, 2, 'task was called twice');
              assert.ok(doneHandler.calledOnce, 'done was called once');
            });
        });
      });

      describe('with max retries (count)', function () {
        var prevDelay;
        var prevCount;
        before(function () {
          prevDelay = process.env.WORKER_MAX_RETRY_DELAY;
          prevCount = process.env.WORKER_MAX_RETRIES;
          process.env.WORKER_MAX_RETRY_DELAY = 2;
          process.env.WORKER_MAX_RETRIES = 2;
        });
        after(function () {
          process.env.WORKER_MAX_RETRY_DELAY = prevDelay;
          process.env.WORKER_MAX_RETRIES = prevCount;
        });
        it('should log TaskMaxRetriesError and call done', function () {
          taskHandler = sinon.stub().throws(new Error('foobar'));
          doneHandler = sinon.stub();
          return assert.isFulfilled(worker.run())
            .then(function () {
              assert.equal(worker.attempt, 3, 'attempts increased');
              assert.equal(taskHandler.callCount, 2, 'task was called twice');
              assert.ok(doneHandler.calledOnce, 'done was called once');
              assert.ok(error.log.calledOnce, 'error called');
              assert.instanceOf(
                error.log.getCall(0).args.pop(),
                TaskMaxRetriesError);
              delete process.env.WORKER_MAX_RETRY_DELAY;
            });
        });
      });
    });

    describe('errored behavior', function () {
      it('should catch TaskFatalError, not retry, and call done', function () {
        taskHandler = sinon.stub().throws(new TaskFatalError('foobar'));
        doneHandler = sinon.stub();
        return assert.isFulfilled(worker.run())
          .then(function () {
            assert.ok(taskHandler.calledOnce, 'task was called once');
            assert.ok(doneHandler.calledOnce, 'done was called once');
          });
      });
      it('should retry if task throws Error', function () {
        taskHandler = sinon.stub();
        taskHandler.onFirstCall().throws(new Error('foobar'));
        doneHandler = sinon.stub();
        return assert.isFulfilled(worker.run())
          .then(function () {
            assert.equal(taskHandler.callCount, 2, 'task was called twice');
            assert.ok(doneHandler.calledOnce, 'done was called once');
          });
      });
      it('should retry if task throws TaskError', function () {
        taskHandler = sinon.stub();
        taskHandler.onFirstCall().throws(new TaskError('foobar'));
        doneHandler = sinon.stub();
        return assert.isFulfilled(worker.run())
          .then(function () {
            assert.equal(taskHandler.callCount, 2, 'task was called twice');
            assert.ok(doneHandler.calledOnce, 'done was called once');
          });
      });
    });
  });
});
