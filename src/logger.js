/* @flow */
/* global Stream */
'use strict'

const bunyan = require('bunyan')

/**
 * Bunyan logger for ponos.
 *
 * @private
 * @author Bryan Kendall
 * @module ponos/lib/logger
 */
module.exports = bunyan.createLogger({
  name: 'ponos',
  streams: _getStreams(),
  serializers: bunyan.stdSerializers
})

// Expose get streams for unit testing
module.exports._getStreams = _getStreams

/**
 * Streams for ponos's bunyan logger.
 * @private
 * @return {array} An array of streams for the bunyan logger
 */
function _getStreams (): Array<Stream> {
  const logLevel = process.env.LOG_LEVEL
  const level = parseInt(logLevel, 10) || logLevel || 'info'
  return [
    {
      level,
      stream: process.stdout
    }
  ]
}
