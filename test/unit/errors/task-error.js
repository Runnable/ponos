'use strict'

const chai = require('chai')
const WorkerError = require('error-cat/errors/worker-error')

const assert = chai.assert

const TaskError = require('../../../lib/errors/task-error')

describe('TaskError', () => {
  const mockJob = { foo: 'bar' }

  it('should inherit from error-cat~WorkerError', () => {
    const err = new TaskError()
    assert.instanceOf(err, TaskError)
    assert.instanceOf(err, WorkerError)
  })

  it('should set the message correctly and set data', () => {
    const err = new TaskError('a-queue', 'had an error', mockJob)
    assert.equal(err.message, 'a-queue: had an error')
  })

  it('should set the data correctly', () => {
    const err = new TaskError('a-queue', 'had an error', mockJob)
    assert.deepEqual(err.data, { queue: 'a-queue', job: mockJob })
  })

  it('should set custom data on the error', () => {
    const err = new TaskError(
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
