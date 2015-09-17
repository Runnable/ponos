'use strict';

var Server = require('./lib/server');

module.exports = {
  server: function (opts) { return new Server(opts); },
  // Errors:
  TaskError: require('./lib/errors/task-error'),
  TaskFatalError: require('./lib/errors/task-fatal-error'),
  TaskMaxRetriesError: require('./lib/errors/task-max-retries-error')
};
