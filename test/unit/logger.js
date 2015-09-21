'use strict';

var chai = require('chai');
chai.use(require('chai-as-promised'));
var assert = chai.assert;

var log = require('../../lib/logger');
var Bunyan2Loggly = require('bunyan-loggly').Bunyan2Loggly;

describe('logger', function () {
  it('should use return one stream by default', function () {
    var streams = log.getStreams();
    assert.lengthOf(streams, 1);
    assert.equal(streams[0].stream, process.stdout);
  });
  it('should be able to add loggly', function () {
    var prevLoggly = process.env.LOGGLY_TOKEN;
    process.env.LOGGLY_TOKEN = 4;
    var streams = log.getStreams();
    assert.lengthOf(streams, 2);
    assert.equal(streams[0].stream, process.stdout);
    assert.instanceOf(streams[1].stream, Bunyan2Loggly);
    process.env.LOGGLY_TOKEN = prevLoggly;
  });
});
