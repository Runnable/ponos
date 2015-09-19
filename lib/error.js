'use strict';

var util = require('util');
var ErrorCat = require('error-cat');
var log = require('./logger');

/**
 * Custom ErrorCat error handler.
 * @class
 */
function PonosErrorCat () {
  ErrorCat.apply(this, arguments);
}
util.inherits(PonosErrorCat, ErrorCat);

/**
 * Logs errors via default ponos logger.
 * @param {error} err PonosErrorCat to log.
 * @see error-cat
 */
PonosErrorCat.prototype.log = function (err) {
  log.error({ err: err }, err.message);
};

/**
 * PonosErrorCat handling via error-cat.
 * @module ponos:error
 * @author Ryan Sandor Richards
 */
module.exports = new PonosErrorCat();
