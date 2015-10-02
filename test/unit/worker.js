'use strict';

var chai = require('chai');
chai.use(require('chai-as-promised'));
var assert = chai.assert;
var sinon = require('sinon');

var Promise = require('bluebird');
var TaskError = require('../../lib/errors/task-error');
var TaskFatalError = require('../../lib/errors/task-fatal-error');
var TimeoutError = Promise.TimeoutError;
var Worker = require('../../lib/worker');
var assign = require('101/assign');

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
      assert.ok(Worker.prototype.run.calledOnce, '.run called');
    });

    it('should hold the job if runNow is not true', function () {
      var testOpts = assign({ runNow: false }, opts);
      Worker.create(testOpts);
      assert.notOk(Worker.prototype.run.calledOnce, '.run not called');
    });

    it('should default the timeout to not exist', function () {
      var w = Worker.create(opts);
      assert.equal(w.msTimeout, 0, 'set the timeout correctly');
    });

    describe('with worker timeout', function () {
      var prevTimeout;
      before(function () {
        prevTimeout = process.env.WORKER_TIMEOUT;
        process.env.WORKER_TIMEOUT = 4000;
      });
      after(function () { process.env.WORKER_TIMEOUT = prevTimeout; });

      it('should use the environment timeout', function () {
        var w = Worker.create(opts);
        assert.equal(w.msTimeout, 4 * 1000, 'set the timeout correctly');
      });

      it('should throw when given a non-integer', function () {
        opts.msTimeout = 'foobar';
        assert.throws(function () {
          Worker.create(opts);
        });
      });
    });
  });

  describe('run', function () {
    var worker;
    beforeEach(function () {
      opts.runNow = false;
      worker = Worker.create(opts);
    });

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

      describe('with worker timeout', function () {
        var prevTimeout;
        before(function () {
          prevTimeout = process.env.WORKER_TIMEOUT;
          process.env.WORKER_TIMEOUT = 10;
        });
        after(function () { process.env.WORKER_TIMEOUT = prevTimeout; });

        it('should timeout the job', function () {
          // we are going to replace the handler w/ a stub
          taskHandler = function () {
            taskHandler = sinon.stub();
            return Promise.resolve().delay(25);
          };
          doneHandler = sinon.stub();
          return assert.isFulfilled(worker.run())
            .then(function () {
              // this is asserting taskHandler called once, but was twice
              assert.ok(taskHandler.calledOnce, 'task was called once');
              assert.ok(doneHandler.calledOnce, 'done was called once');
            });
        });
      });

      describe('with max retry delay', function () {
        var prevDelay;
        before(function () {
          prevDelay = process.env.WORKER_MAX_RETRY_DELAY;
          process.env.WORKER_MAX_RETRY_DELAY = 4;
        });
        after(function () { process.env.WORKER_MAX_RETRY_DELAY = prevDelay; });

        it('should exponentially back off up to max', function () {
          var initDelay = worker.retryDelay;
          taskHandler = sinon.stub();
          taskHandler.onFirstCall().throws(new Error('foobar'));
          taskHandler.onSecondCall().throws(new Error('foobar'));
          taskHandler.onThirdCall().throws(new Error('foobar'));
          doneHandler = sinon.stub();
          return assert.isFulfilled(worker.run())
            .then(function () {
              assert.notEqual(initDelay, worker.retryDelay, 'delay increased');
              assert.equal(worker.retryDelay, 4, 'delay increased to max');
              assert.equal(taskHandler.callCount, 4, 'task was called twice');
              assert.ok(doneHandler.calledOnce, 'done was called once');
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
      it('should retry if task throws TimeoutError', function () {
        taskHandler = sinon.stub();
        taskHandler.onFirstCall().throws(new TimeoutError());
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
