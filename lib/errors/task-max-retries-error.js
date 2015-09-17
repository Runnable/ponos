'use strict';

var util = require('util');
var TaskError = require('./task-error');

function TaskMaxRetriesError (queue, data) {
  var message = 'Worker max retries reached';
  TaskError.call(this, queue, message, data);
}
util.inherits(TaskMaxRetriesError, TaskError);

/**
 * Error that indicates that the max retries have been reached. Mostly for
 * internal usage.
 * @author Bryan Kendall
 * @module ponos:errors
 */
module.exports = TaskMaxRetriesError;
