'use strict'

const chai = require('chai')

const log = require('../../lib/logger')

const assert = chai.assert

describe('logger', function () {
  it('should use return one stream by default', function () {
    const streams = log._getStreams()
    assert.lengthOf(streams, 1)
    assert.equal(streams[0].stream, process.stdout)
  })
})
