'use strict'
const joi = require('joi')
const Limiter = require('ratelimiter')
const Promise = require('bluebird')
const redis = require('redis')

const optsSchema = joi.object({
  port: joi.string().required(),
  host: joi.string().required(),
  durationMs: joi.number().integer().min(0).required()
})

module.exports = class RedisRateLimiter {
  port: string;
  host: string;

  /**
   * creates RedisRateLimiter object
   * @param  {Object} opts  redis connection options
   * @param  {String} opts.port redis connection port
   * @param  {String} opts.host redis connection host
   * @return {RedisRateLimiter}
   */
  constructor (opts: Object) {
    this.port = opts.port
    this.host = opts.host
    // default to 1 second
    this.durationMs = opts.durationMs || 1000
    joi.assert(this, optsSchema)
  }

  /**
   * Connect redis client to redis
   * @return {Promise}
   * @resolves {undefined} When connection is ready
   * @reject {Error} When there was an error connecting
   */
  connect () {
    return Promise.asCallback((cb) => {
      this.client = redis.createClient(this.port, this.host)
      this.client.on('error', cb)
      this.client.on('ready', cb)
    })
  }

  /**
   * Ensure promise's get resolved at a given rate
   * @param  {String} name  name of task or queue to limit
   * @param  {Object} opts  rate limiting options
   * @param  {String} opts.maxOperations  max number of operations per duration
   * @param  {String} opts.duration  time period to limit operations in
   * @return {Promise}
   */
  limit (name: string, opts: Object) {
    const limiter = new Limiter({
      id: opts.name,
      db: this.client,
      max: opts.maxOperations,
      duration: opts.durationMs || this.durationMs
    })

    // is max operations not set, do not limit
    if (!opts.maxOperations) {
      return Promise.resolve()
    }
    return Promise.asCallback((cb) => {
      limiter.get(cb)
    })
    .then((limit) => {
      if (!limit.remaining) {
        return Promise
          .delay((limit.reset * 1000) - Date.now() | 0)
          .then(this.limit(name))
      }
    })
  }
}
