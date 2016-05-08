'use strict'

const chai = require('chai')
const WorkerStopError = require('error-cat/errors/worker-stop-error')

const assert = chai.assert

const TaskFatalError = require('../../../lib/errors/task-fatal-error')

describe('TaskFatalError', () => {
  const mockJob = { foo: 'bar' }

  it('should inherit from error-cat~WorkerStopError', () => {
    const err = new TaskFatalError()
    assert.instanceOf(err, TaskFatalError)
    assert.instanceOf(err, WorkerStopError)
  })

  it('should set the message correctly and set data', () => {
    const err = new TaskFatalError('a-queue', 'had an error', mockJob)
    assert.equal(err.message, 'a-queue: had an error')
  })

  it('should set the data correctly', () => {
    const err = new TaskFatalError('a-queue', 'had an error', mockJob)
    assert.deepEqual(err.data, { queue: 'a-queue', job: mockJob })
  })

  it('should set custom data on the error', () => {
    const err = new TaskFatalError(
      'a-queue',
      'had an error',
      mockJob,
      { hello: 'world' }
    )
    assert.deepEqual(err.data, {
      queue: 'a-queue',
      job: mockJob,
      hello: 'world'
    })
  })
})
