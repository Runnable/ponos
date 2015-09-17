'use strict';

var util = require('util');
var ErrorCat = require('error-cat');
var log = require('./logger');

/**
 * Custom ErrorCat error handler.
 * @class
 */
function PonosError () {
  ErrorCat.apply(this, arguments);
}
util.inherits(PonosError, ErrorCat);

/**
 * Logs errors via default ponos logger.
 * @param {error} err PonosError to log.
 * @see error-cat
 */
PonosError.prototype.log = function (err) {
  log.error({ err: err }, err.message);
};

/**
 * PonosError handling via error-cat.
 * @module ponos:error
 * @author Ryan Sandor Richards
 */
module.exports = new PonosError();
