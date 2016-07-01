'use strict'

const chai = require('chai')

const log = require('../../src/logger')

const assert = chai.assert

describe('logger', () => {
  it('should use return one stream by default', () => {
    const streams = log._getStreams()
    assert.lengthOf(streams, 1)
    assert.equal(streams[0].stream, process.stdout)
  })

  describe('when LOG_LEVEL is not defined', () => {
    let prevLevel

    beforeEach(() => {
      prevLevel = process.env.LOG_LEVEL
      process.env.LOG_LEVEL = undefined
    })

    afterEach(() => {
      process.env.LOG_LEVEL = prevLevel
    })

    it('should default info level', () => {
      const streams = log._getStreams()
      assert.equal(streams[0].level, 'info')
    })
  })
})
