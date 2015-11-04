'use strict';

var chai = require('chai');
var assert = chai.assert;

var TaskError = require('../../../lib/errors/task-error');

describe('TaskError', function () {
  it('should inherit from Error', function () {
    var err = new TaskError();
    assert.instanceOf(err, TaskError);
    assert.instanceOf(err, Error);
  });
  it('should set the message correctly and default data', function () {
    var err = new TaskError('a-queue', 'had an error');
    assert.equal(err.message, 'a-queue: had an error');
    assert.deepEqual(err.data, { queue: 'a-queue' });
  });
  it('should set custom data on the error', function () {
    var err = new TaskError('a-queue', 'had an error', { hello: 'world' });
    assert.deepEqual(err.data, {
      queue: 'a-queue',
      hello: 'world'
    });
  });
});
