'use strict';

var chai = require('chai');
chai.use(require('chai-as-promised'));
var assert = chai.assert;
var sinon = require('sinon');

var error = require('../../lib/error');
var log = require('../../lib/logger');

describe('PonosError', function () {
  it('should log to the logger with err', function () {
    sinon.stub(log, 'error');
    var err = new Error('foobar');
    error.log(err);
    assert.ok(log.error.calledOnce, 'log.error called once');
    var args = log.error.getCall(0).args;
    assert.equal(args.shift().err, err);
    assert.equal(args.shift(), 'foobar');
    log.error.restore();
  });
});
