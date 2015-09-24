'use strict';

var chai = require('chai');
chai.use(require('chai-as-promised'));
var assert = chai.assert;

var TaskError = require('../../../lib/errors/task-error');
var TaskFatalError = require('../../../lib/errors/task-fatal-error');

describe('TaskFatalError', function () {
  it('should inherit from Error and TaskError', function () {
    var err = new TaskFatalError();
    assert.instanceOf(err, TaskFatalError);
    assert.instanceOf(err, TaskError);
    assert.instanceOf(err, Error);
  });
  it('should set custom data on the error', function () {
    var err = new TaskFatalError('a-queue', 'had an error', { hello: 'world' });
    assert.equal(err.message, 'a-queue: had an error');
    assert.deepEqual(err.data, {
      queue: 'a-queue',
      hello: 'world'
    });
  });
});
