'use strict'
const Promise = require('bluebird')
const redis = require('redis')
const Limiter = require('ratelimiter')

module.exports = class RedisRateLimiter {
  constructor (opts) {
    this.port = opts.port
    this.host = opts.host
    this.limiters = {}
  }

  connect () {
    return Promise.asCallback((cb) => {
      this.client = redis.createClient(this.port, this.host)
      this.client.on('error', cb)
      this.client.on('ready', cb)
    })
  }

  addLimiter (opts) {
    this[opts.name] = {
      limiter: new Limiter({
        id: opts.name,
        db: this.client,
        max: opts.rateLimitMax,
        duration: opts.rateLimitDuration
      }),
      queue: []
    }
  }

  waitForLimit (name) {
    this[name].queue.push()

    return Promise.try(function waitForSpace () {
      return Promise.asCallback((cb) => {
        this[name].get(cb)
      })
      .then((limit) => {
        if (!limit.remaining) {
          return Promise
            .delay((limit.reset * 1000) - Date.now() | 0)
            .then(this.waitForLimit)
        }
      })
    })
  }
}
