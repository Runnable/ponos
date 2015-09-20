'use strict';

var chai = require('chai');
var assert = chai.assert;

// Ponos Tooling
var ponos = require('../../');
var testWorker = require('./fixtures/worker');
var testWorkerEmitter = testWorker.emitter;

describe('Basic Example', function () {
  var server;
  before(function (done) {
    var tasks = {
      'ponos-test:one': testWorker
    };
    server = new ponos.Server({ queues: Object.keys(tasks) });
    server.setAllTasks(tasks)
      .then(server.start())
      .then(function () { done(); });
  });
  after(function (done) { server.stop().then(function () { done(); }); });

  it('should queue a task that triggers an event', function (done) {
    testWorkerEmitter.on('task', function (data) {
      testWorkerEmitter.removeAllListeners('task');
      assert.equal(data.data, 'hello world');
      done();
    });
    testWorkerEmitter.on('error', function (err) {
      testWorkerEmitter.removeAllListeners('task');
      done(err);
    });
    var data = {
      queue: 'ponos-test:two',
      message: 'hello world'
    };
    server.hermes.publish('ponos-test:one', data);
  });
});
