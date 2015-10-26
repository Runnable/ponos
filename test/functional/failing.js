'use strict';

var chai = require('chai');
var assert = chai.assert;
var async = require('async');
var sinon = require('sinon');

// Ponos Tooling
var ponos = require('../../');
var TaskFatalError = ponos.TaskFatalError;
var testWorker = require('./fixtures/worker');
var testWorkerEmitter = testWorker.emitter;

// require the Worker class so we can verify the task is running
var _Worker = require('../../lib/worker');

/*
 *  In this example, we are going to pass an invalid job to the worker that will
 *  throw a TaskFatalError, acknowledge the job, and not run it a second time.
 */
describe('Basic Failing Task', function () {
  var server;
  before(function (done) {
    sinon.spy(_Worker.prototype, 'run');
    sinon.spy(_Worker.prototype, '_reportError');
    var tasks = {
      'ponos-test:one': testWorker
    };
    server = new ponos.Server({ queues: Object.keys(tasks) });
    server.setAllTasks(tasks).start()
      .then(function () {
        assert.notOk(_Worker.prototype.run.called, '.run should not be called');
        done();
      })
      .catch(done);
  });
  after(function (done) {
    server.stop()
      .then(function () {
        _Worker.prototype.run.restore();
        _Worker.prototype._reportError.restore();
        done();
      });
  });

  var job = {
    eventName: 'will-never-emit'
  };

  // Before we run the test, let's assert that our task fails with the job.
  // This should be _rejected_ with an error.
  before(function () {
    var testWorkerPromise = testWorker(job)
      .catch(function (err) {
        // extra assertion that it's still a TaskFatalError
        assert.instanceOf(err, TaskFatalError);
        throw err;
      });
    return assert.isRejected(testWorkerPromise, /message.+required/);
  });

  it('should fail once and not be re-run', function (done) {
    testWorkerEmitter.on('will-never-emit', function () {
      done(new Error('failing worker should not have emitted'));
    });
    server.hermes.publish('ponos-test:one', job);

    // wait until .run is called
    async.until(
      function () { return _Worker.prototype.run.calledOnce; },
      function (cb) { setTimeout(cb, 5); },
      function () {
        assert.ok(_Worker.prototype.run.calledOnce, '.run called once');
        /*
         *  We can get the promise and assure that it was fulfilled!
         *  This should be _fulfilled_ because it threw a TaskFatalError and
         *  acknowledged that the task was completed (even though the task
         *  rejected with an error);
         */
        var workerRunPromise = _Worker.prototype.run.firstCall.returnValue;
        assert.isFulfilled(workerRunPromise);
        assert.ok(
          _Worker.prototype._reportError.calledOnce,
          'worker._reportError called once'
        );
        var err = _Worker.prototype._reportError.firstCall.args[0];
        assert.instanceOf(err, TaskFatalError);
        assert.match(err, /message.+required/);
        done();
      });
  });
});
