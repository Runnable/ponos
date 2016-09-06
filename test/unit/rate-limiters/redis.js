'use strict'
const chai = require('chai')
const Promise = require('bluebird')
const put = require('101/put')
const RateLimiter = require('ratelimiter')
const redis = require('redis')
const sinon = require('sinon')

const RedisRateLimiter = require('../../../src/rate-limiters/redis')
const logger = require('../../../src/logger')

const assert = chai.assert

describe('redis', () => {
  let testOpts
  let testRedisRateLimiter

  beforeEach(() => {
    testOpts = {
      port: '123',
      host: 'localhost',
      log: logger
    }
    testRedisRateLimiter = new RedisRateLimiter(testOpts)
  })

  describe('constructor', () => {
    let out
    it('should throw validation error if missing port', () => {
      delete testOpts.port
      assert.throws(() => {
        out = new RedisRateLimiter(testOpts)
      }, Error, /"port" is required/)
      assert.isUndefined(out)
    })

    it('should throw validation error if missing host', () => {
      delete testOpts.host
      assert.throws(() => {
        out = new RedisRateLimiter(testOpts)
      }, Error, /"host" is required/)
      assert.isUndefined(out)
    })

    it('should throw validation error if missing log', () => {
      delete testOpts.log
      assert.throws(() => {
        out = new RedisRateLimiter(testOpts)
      }, Error, /"log" is required/)
      assert.isUndefined(out)
    })

    it('should default durationMs to 1000', () => {
      out = new RedisRateLimiter(testOpts)
      assert.equal(out.durationMs, 1000)
    })

    it('should use passed durationMs', () => {
      out = new RedisRateLimiter(put({
        durationMs: 1738
      }, testOpts))
      assert.equal(out.durationMs, 1738)
    })
  }) // constructor

  describe('connect', () => {
    let onStub
    beforeEach(() => {
      onStub = sinon.stub()
      sinon.stub(redis, 'createClient').returns({
        on: onStub
      })
    })

    afterEach(() => {
      redis.createClient.restore()
    })

    it('should create redis client', () => {
      onStub.onFirstCall().yieldsAsync()
      return assert.isFulfilled(testRedisRateLimiter.connect())
        .then(() => {
          sinon.assert.calledOnce(redis.createClient)
          sinon.assert.calledWith(redis.createClient, testOpts.port, testOpts.host)
        })
    })

    it('should attach error handler', () => {
      onStub.onFirstCall().yieldsAsync()
      return assert.isFulfilled(testRedisRateLimiter.connect())
        .then(() => {
          sinon.assert.calledTwice(onStub)
          sinon.assert.calledWith(onStub, 'error', sinon.match.func)
          sinon.assert.calledWith(onStub, 'ready', sinon.match.func)
        })
    })

    it('should attach error handler', () => {
      onStub.onSecondCall().yieldsAsync()
      return assert.isFulfilled(testRedisRateLimiter.connect())
        .then(() => {
          sinon.assert.calledTwice(onStub)
          sinon.assert.calledWith(onStub, 'error', sinon.match.func)
          sinon.assert.calledWith(onStub, 'ready', sinon.match.func)
        })
    })
  }) // end connect

  describe('limit', () => {
    const testLimitOpts = {
      durationMs: 2000,
      maxOperations: 3
    }

    const testName = 'Shiva'
    let stubTime
    beforeEach(() => {
      sinon.spy(Promise, 'delay')
      stubTime = sinon.useFakeTimers()
      sinon.stub(RateLimiter.prototype, 'get')
      sinon.spy(testRedisRateLimiter, 'limit')
      testRedisRateLimiter.client = {}
    })

    afterEach(() => {
      Promise.delay.restore()
      stubTime.restore()
      RateLimiter.prototype.get.restore()
    })

    it('should resolve if maxOperations not defined', () => {
      return assert.isFulfilled(testRedisRateLimiter.limit(testName, {}))
        .then(() => {
          sinon.assert.notCalled(RateLimiter.prototype.get)
        })
    })

    it('should resolve if items remaining', () => {
      RateLimiter.prototype.get.yieldsAsync(null, {
        remaining: 5
      })
      return assert.isFulfilled(testRedisRateLimiter.limit(testName, testLimitOpts))
        .then(() => {
          sinon.assert.calledOnce(RateLimiter.prototype.get)
          sinon.assert.calledOnce(testRedisRateLimiter.limit)
        })
    })

    it('should delay until there is space', () => {
      RateLimiter.prototype.get.onFirstCall().yieldsAsync(null, {
        remaining: 0
      })
      RateLimiter.prototype.get.onSecondCall().yieldsAsync(null, {
        remaining: 0
      })
      RateLimiter.prototype.get.onThirdCall().yieldsAsync(null, {
        remaining: 1
      })

      return assert.isFulfilled(Promise.all([
        testRedisRateLimiter.limit(testName, testLimitOpts)
          .then(() => {
            sinon.assert.calledThrice(RateLimiter.prototype.get)
            sinon.assert.calledThrice(testRedisRateLimiter.limit)
            sinon.assert.calledTwice(Promise.delay)
            sinon.assert.alwaysCalledWith(Promise.delay, testLimitOpts.durationMs / 2)
          }),
        Promise.try(function loop () {
          if (Promise.delay.callCount !== 1) {
            return Promise.fromCallback(process.nextTick).then(loop)
          }
        })
        .then(() => {
          sinon.assert.calledOnce(RateLimiter.prototype.get)
          sinon.assert.calledOnce(testRedisRateLimiter.limit)
          sinon.assert.alwaysCalledWith(testRedisRateLimiter.limit, testName, testLimitOpts)
          sinon.assert.calledOnce(Promise.delay)
          sinon.assert.alwaysCalledWith(Promise.delay, testLimitOpts.durationMs / 2)
          stubTime.tick(testLimitOpts.durationMs)
        })
        .then(function loop () {
          if (Promise.delay.callCount !== 2) {
            return Promise.fromCallback(process.nextTick).then(loop)
          }
        })
        .then(() => {
          sinon.assert.calledTwice(RateLimiter.prototype.get)
          sinon.assert.calledTwice(testRedisRateLimiter.limit)
          sinon.assert.calledTwice(Promise.delay)
          sinon.assert.alwaysCalledWith(Promise.delay, testLimitOpts.durationMs / 2)
          stubTime.tick(testLimitOpts.durationMs)
        })
      ]))
    })
  }) // end limit
})
