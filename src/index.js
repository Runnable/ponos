'use strict'

const Server = require('./server')
const TaskError = require('./errors/task-error')
const TaskFatalError = require('./errors/task-fatal-error')

module.exports = {
  Server: Server,
  // Errors:
  TaskError: TaskError,
  TaskFatalError: TaskFatalError
}
