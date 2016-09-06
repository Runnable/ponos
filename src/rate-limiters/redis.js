/* @flow */
/* global Bluebird$Promise Logger RedisClient*/
'use strict'
const joi = require('joi')
const RateLimiter = require('ratelimiter')
const Promise = require('bluebird')
const redis = require('redis')

const optsSchema = joi.object({
  durationMs: joi.number().integer().min(0).required(),
  host: joi.string().required(),
  log: joi.object().required(),
  port: joi.string().required()
})

module.exports = class RedisRateLimiter {
  port: string;
  host: string;
  log: Logger;
  durationMs: number;
  client: RedisClient;

  /**
   * creates RedisRateLimiter object
   * @param  {Object} opts  redis connection options
   * @param  {String} opts.port redis connection port
   * @param  {String} opts.host redis connection host
   * @param  {String} opts.log  worker server logger`
   * @return {RedisRateLimiter}
   */
  constructor (opts: Object) {
    this.port = opts.port
    this.host = opts.host
    this.log = opts.log
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
  connect (): Bluebird$Promise<*> {
    return Promise.fromCallback((cb) => {
      this.log.trace('connecting to redis')
      this.client = redis.createClient(this.port, this.host)
      this.client.on('error', cb)
      this.client.on('ready', cb)
    })
  }

  /**
   * Ensure promise's get resolved at a given rate
   * @param  {String} queueName  queueName of task or event to limit
   * @param  {Object} opts  rate limiting options
   * @param  {String} opts.maxOperations  max number of operations per duration
   * @param  {String} opts.durationMs  time period to limit operations in milliseconds
   * @return {Promise}
   */
  limit (queueName: string, opts: Object): Bluebird$Promise<void> {
    const log = this.log.child({ queueName: queueName, opts: opts })
    const durationMs = opts.durationMs || this.durationMs
    const limiter = new RateLimiter({
      id: queueName,
      db: this.client,
      max: opts.maxOperations,
      duration: durationMs
    })
    // is max operations not set, do not limit
    if (!opts.maxOperations) {
      return Promise.resolve()
    }
    log.trace('checking rate limit')
    return Promise.fromCallback((cb) => {
      limiter.get(cb)
    })
    .then((limitProperties) => {
      if (!limitProperties.remaining) {
        const delayTimeMs = Math.floor(durationMs / 2)
        log.warn({ limitProperties: limitProperties, delayTimeMs: delayTimeMs }, 'over the limit, delaying')
        return Promise
          .delay(delayTimeMs)
          .then(() => {
            return this.limit(queueName, opts)
          })
      }
      log.trace({ limitProperties: limitProperties }, 'under limit')
    })
  }
}
