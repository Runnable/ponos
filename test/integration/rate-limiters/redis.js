'use strict'
const chai = require('chai')
const Promise = require('bluebird')
const redis = require('redis')
const sinon = require('sinon')

const logger = require('../../../src/logger')
const RedisRateLimiter = require('../../../src/rate-limiters/redis')

const assert = chai.assert

describe('rate-limiters/redis integration test', () => {
  let redisRateLimiter
  const testClient = redis.createClient('6379', 'localhost')

  beforeEach((done) => {
    redisRateLimiter = new RedisRateLimiter({
      log: logger.child({ module: 'ponos:test' })
    })
    sinon.spy(Promise, 'delay')
    redisRateLimiter.connect()
      .then(() => {
        testClient.flushall(done)
      })
  })

  afterEach(() => {
    Promise.delay.restore()
  })

  it('should immediately resolve', () => {
    return assert.isFulfilled(redisRateLimiter.limit('test', {}))
  })

  it('should only call 1 in rate limit period', () => {
    let count = 0
    const tasks = []
    for (var i = 0; i < 5; i++) {
      tasks.push(redisRateLimiter.limit('test', {
        maxOperations: 1,
        durationMs: 500
      }).then(() => { count++ }))
    }
    return Promise.all(tasks)
    .timeout(50)
    .catch(Promise.TimeoutError, () => {
      assert.equal(count, 1)
    })
  })

  it('should limit to 5 during period', () => {
    let count = 0
    const tasks = []
    for (var i = 0; i < 10; i++) {
      tasks.push(redisRateLimiter.limit('test', {
        maxOperations: 5,
        durationMs: 500
      }).then(() => { count++ }))
    }
    return Promise.all(tasks)
    .timeout(800)
    .catch(Promise.TimeoutError, () => {
      assert.equal(count, 5)
    })
  })
}) // end rate-limiters/redis integration test
