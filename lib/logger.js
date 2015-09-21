'use strict';

var exists = require('101/exists');
var bunyan = require('bunyan');
var Bunyan2Loggly = require('bunyan-loggly').Bunyan2Loggly;

/**
 * Bunyan logger for ponos.
 * @author Bryan Kendall
 * @module ponos:logger
 */
module.exports = bunyan.createLogger({
  name: 'ponos',
  streams: getStreams()
});

// Expose get streams for unit testing
module.exports.getStreams = getStreams;

/**
 * Streams for ponos's bunyan logger.
 * @return {array} An array of streams for the bunyan logger
 */
function getStreams () {
  var streams = [{
    level: process.env.LOG_LEVEL_STDOUT,
    stream: process.stdout
  }];

  if (exists(process.env.LOGGLY_TOKEN)) {
    streams.push({
      level: process.env.LOG_LEVEL_STDOUT,
      stream: new Bunyan2Loggly({
        token: process.env.LOGGLY_TOKEN,
        subdomain: 'sandboxes'
      }, process.env.BUNYAN_BATCH_LOG_COUNT),
      type: 'raw'
    });
  }

  return streams;
}
