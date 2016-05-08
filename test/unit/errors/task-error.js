'use strict'

const chai = require('chai')

const assert = chai.assert

const TaskError = require('../../../src/errors/task-error')

describe('TaskError', () => {
  it('should inherit from Error', () => {
    const err = new TaskError()
    assert.instanceOf(err, TaskError)
    assert.instanceOf(err, Error)
  })

  it('should set the message correctly and default data', () => {
    const err = new TaskError('a-queue', 'had an error')
    assert.equal(err.message, 'a-queue: had an error')
    assert.deepEqual(err.data, { queue: 'a-queue' })
  })

  it('should set custom data on the error', () => {
    const err = new TaskError('a-queue', 'had an error', { hello: 'world' })
    assert.deepEqual(err.data, {
      queue: 'a-queue',
      hello: 'world'
    })
  })
})
