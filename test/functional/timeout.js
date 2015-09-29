'use strict';

var chai = require('chai');
chai.use(require('chai-as-promised'));
var assert = chai.assert;
var sinon = require('sinon');

// Ponos Tooling
var ponos = require('../../');
var TimeoutError = require('bluebird').TimeoutError;
var testWorker = require('./fixtures/timeout-worker');
var testWorkerEmitter = testWorker.emitter;

// require the Worker class so we can verify the task is running
var _Worker = require('../../lib/worker');
// require the error module so we can see the error printed
var _Bunyan = require('bunyan');

/*
 *  In this example, we are going to have a job handler that times out at
 *  decreasing intervals, throwing TimeoutErrors, until it passes.
 */
describe('Basic Timeout Task', function () {
  this.timeout(3500);
  var server;
  var prevTimeout;
  before(function (done) {
    prevTimeout = process.env.WORKER_TIMEOUT;
    process.env.WORKER_TIMEOUT = 1;
    sinon.spy(_Worker.prototype, 'run');
    sinon.spy(_Bunyan.prototype, 'warn');
    var tasks = {
      'ponos-test:one': testWorker
    };
    server = new ponos.Server({ queues: Object.keys(tasks) });
    server.setAllTasks(tasks)
      .then(server.start())
      .then(function () {
        assert.notOk(_Worker.prototype.run.called, '.run should not be called');
        done();
      })
      .catch(done);
  });
  after(function (done) {
    server.stop()
      .then(function () {
        process.env.WORKER_TIMEOUT = prevTimeout;
        _Worker.prototype.run.restore();
        _Bunyan.prototype.warn.restore();
        done();
      });
  });

  var job = {
    eventName: 'did-not-time-out',
    message: 'hello world'
  };

  it('should fail twice and pass the third time', function (done) {
    testWorkerEmitter.on('did-not-time-out', function () {
      // setTimeout so the worker can resolve
      setTimeout(function () {
        // this signals to us that we are done!
        assert.ok(_Worker.prototype.run.calledThrice, '.run called thrice');
        /*
         *  We can get the promise and assure that it was fulfilled!
         *  It was run three times and all three should be fulfilled.
         */
        [
          _Worker.prototype.run.firstCall.returnValue,
          _Worker.prototype.run.secondCall.returnValue,
          _Worker.prototype.run.thirdCall.returnValue
        ].forEach(function (p) { assert.isFulfilled(p); });
        // and, make sure the error module has logged the TimeoutError twice
        // have to do a bit of weird filtering, but this is correct. Long story
        // short, log.warn is called a couple times, but we just want to make
        // sure the 'task timed out' message is just twice (the number of times
        // this worker failed).
        var errors = _Bunyan.prototype.warn.args.reduce(function (memo, args) {
          var checkArgs = args.filter(function (arg) {
            return /task timed out/i.test(arg);
          });
          if (checkArgs.length) { memo.push(args.shift().err); }
          return memo;
        }, []);
        errors.forEach(function (err) {
          assert.instanceOf(err, TimeoutError);
        });
        done();
      }, 50);
    });

    server.hermes.publish('ponos-test:one', job);
  });
});
