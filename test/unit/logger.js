'use strict'

var chai = require('chai')
var assert = chai.assert

var log = require('../../lib/logger')

describe('logger', function () {
  it('should use return one stream by default', function () {
    var streams = log.getStreams()
    assert.lengthOf(streams, 1)
    assert.equal(streams[0].stream, process.stdout)
  })
})
