'use strict';

var chai = require('chai');
chai.use(require('chai-as-promised'));
var assert = chai.assert;

var TaskMaxRetriesError = require('../../../lib/errors/task-max-retries-error');

describe('TaskMaxRetriesError', function () {
  it('should default the message', function () {
    var err = new TaskMaxRetriesError('a-queue');
    assert.instanceOf(err, TaskMaxRetriesError);
    assert.match(err.message, /max retries reached/);
  });
  it('should set custom data on the error', function () {
    var err = new TaskMaxRetriesError('a-queue', { hello: 'world' });
    assert.deepEqual(err.data, {
      queue: 'a-queue',
      hello: 'world'
    });
  });
});
