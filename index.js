'use strict'

module.exports = {
  Server: require('./lib/server'),
  // Errors:
  TaskError: require('./lib/errors/task-error'),
  TaskFatalError: require('./lib/errors/task-fatal-error')
}
