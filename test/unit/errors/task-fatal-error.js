'use strict'

const chai = require('chai')

const assert = chai.assert

const TaskError = require('../../../lib/errors/task-error')
const TaskFatalError = require('../../../lib/errors/task-fatal-error')

describe('TaskFatalError', () => {
  it('should inherit from Error and TaskError', () => {
    const err = new TaskFatalError()
    assert.instanceOf(err, TaskFatalError)
    assert.instanceOf(err, TaskError)
    assert.instanceOf(err, Error)
  })

  it('should set custom data on the error', () => {
    const err = new TaskFatalError('a-queue', 'had an error', { hello: 'world' })
    assert.equal(err.message, 'a-queue: had an error')
    assert.deepEqual(err.data, {
      queue: 'a-queue',
      hello: 'world'
    })
  })
})
