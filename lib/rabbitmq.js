/* @flow */
/* global RabbitMQChannel RabbitMQConnection SubscribeObject */
'use strict'

const amqplib = require('amqplib')
const Immutable = require('immutable')
const isFunction = require('101/is-function')
const Promise = require('bluebird')

const logger = require('./logger')

/**
 * RabbitMQ class.
 *
 * @class
 * @module ponos:rabbitmq
 */
class RabbitMQ {
  channel: RabbitMQChannel;
  connection: RabbitMQConnection;
  consuming: Map<string, string>;
  hostname: string;
  log: Object;
  port: number;
  subscribed: Set<string>;
  subscriptions: Map<string, Function>;

  constructor () {
    this.hostname = 'localhost'
    this.port = 32777
    // this.username = 'guest'
    // this.password = 'guest'
    this.log = logger.child({
      module: 'ponos:rabbitmq',
      hostname: this.hostname,
      port: this.port
    })
    this.subscriptions = new Immutable.Map()
    this.subscribed = new Immutable.Set()
    this.consuming = new Immutable.Map()
    return this
  }

  /**
   * Connect to the RabbitMQ server.
   * @return {Promise} Resolves once connection is establised.
   */
  connect () {
    if (this.connection || this.channel) {
      return Promise.reject(new Error('cannot call connect twice'))
    }
    let authString = ''
    if (this.username && this.password) {
      authString = `${this.username}:${this.password}@`
    }
    const url = `amqp://${authString}${this.hostname}:${this.port}`
    this.log.info({ url: url }, 'connecting')
    return Promise
      .resolve(amqplib.connect(url, {}))
      .catch((err) => {
        this.log.error({ err: err }, 'an error occured while connecting')
        throw err
      })
      .then((conn) => {
        this.connection = conn
        if (!this.connection) {
          throw new Error('error making connection')
        }
        this.log.info('connected')
        this.connection.on('error', this._connectionErrorHandler.bind(this))

        this.log.info('creating channel')
        return Promise.resolve(this.connection.createChannel())
          .catch((err) => {
            this.log.error({ err: err }, 'an error occured creating channel')
            throw err
          })
      })
      .then((channel) => {
        this.log.info('created channel')
        this.channel = channel
        this.channel.on('error', this._channelErrorHandler.bind(this))
      })
  }

  /**
   * Error handler for the RabbitMQ connection.
   * @private
   * @throws
   * @param {object} err Error object from event.
   */
  _connectionErrorHandler (err: Error) {
    this.log.error({ err: err }, 'connection has caused an error')
    throw err
  }

  /**
   * Error handler for the RabbitMQ channel.
   * @private
   * @throws
   * @param {object} err Error object from event.
   */
  _channelErrorHandler (err: Error) {
    this.log.error({ err: err }, 'channel has caused an error')
    throw err
  }

  subscribeToQueue (queue: string, handler: Function) {
    const log = this.log.child({
      method: 'subscribeToQueue',
      queue: queue
    })
    log.info('subscribing to queue')
    if (!this.channel) {
      return Promise.reject(new Error('you must .connect() before subscribing'))
    }
    if (!isFunction(handler)) {
      log.error('handler must be a function')
    }
    if (this.subscribed.has(`queue:::${queue}`)) {
      log.warn('already subscribed to queue')
      return Promise.resolve()
    }
    return Promise
      .resolve(this.channel.assertQueue(queue, { durable: true }))
      .then(() => {
        log.info('queue asserted, binding queue')
        this.subscriptions = this.subscriptions.set(queue, handler)
        this.subscribed = this.subscribed.add(`queue:::${queue}`)
      })
  }

  _subscribeToExchange (opts: SubscribeObject) {
    const log = this.log.child({
      method: '_subscribeToExchange',
      opts: opts
    })
    log.info('subscribing to exchange')
    if (!this.channel) {
      return Promise.reject(new Error('must .connect() before subscribing'))
    }
    if (!opts.exchange || !opts.type || !opts.handler) {
      return Promise.reject(new Error('exchange and handler are required'))
    }
    if (!opts.type === 'topic' && !opts.routingKey) {
      return Promise.reject(new Error('routingKey required for topic exchange'))
    }
    let subscribedKey = `${opts.type}:::${opts.exchange}`
    if (opts.type === 'topic') {
      subscribedKey = `${subscribedKey}:::${opts.routingKey}`
    }
    if (this.subscribed.has(subscribedKey)) {
      log.warn(`already subscribed to ${opts.type} exchange`)
      return Promise.resolve()
    }
    return Promise
      .resolve(
        this.channel.assertExchange(
          opts.exchange,
          opts.type,
          { durable: false }
        )
      )
      .then(() => {
        log.info('exchange asserted')
        let queueName = `ponos.${opts.exchange}`
        if (opts.type === 'topic') {
          queueName = `${queueName}.${opts.routingKey}`
        }
        return Promise
          .resolve(this.channel.assertQueue(queueName, { exclusive: true }))
      })
      .then((queueInfo) => {
        const queue = queueInfo.queue
        log.info({ queue: queue }, 'queue asserted')
        log.info('binding queue')
        if (!opts.routingKey) {
          opts.routingKey = ''
        }
        return Promise
          .resolve(
            this.channel.bindQueue(queue, opts.exchange, opts.routingKey)
          )
          .return(queue)
      })
      .then((queue) => {
        log.info('bound queue')
        this.subscriptions = this.subscriptions.set(queue, opts.handler)
        this.subscribed = this.subscribed.add(subscribedKey)
      })
  }

  subscribeToFanoutExchange (exchange: string, handler: Function) {
    return this._subscribeToExchange({
      exchange: exchange,
      type: 'fanout',
      handler: handler
    })
  }

  subscribeToTopicExchange (
    exchange: string,
    routingKey: string,
    handler: Function
  ) {
    return this._subscribeToExchange({
      exchange: exchange,
      type: 'topic',
      routingKey: routingKey,
      handler: handler
    })
  }

  consume () {
    const log = this.log.child({ method: 'consume' })
    log.info('starting to consume')
    if (!this.channel) {
      throw new Error('you must .connect() before consuming')
    }
    const subscriptions = this.subscriptions
    this.subscriptions = new Immutable.Map()
    const channel = this.channel
    return Promise.map(subscriptions.keySeq(), (queue) => {
      const handler = subscriptions.get(queue)
      log.info({ queue: queue }, 'consuming on queue')
      if (this.consuming.has(queue)) {
        log.warn({ queue: queue }, 'already consuming queue')
        return true
      }
      function wrapper (msg) {
        const job = JSON.parse(msg.content)
        handler(job, () => {
          channel.ack(msg)
        })
      }
      return Promise.resolve(this.channel.consume(queue, wrapper))
        .then((consumeInfo) => {
          this.consuming = this.consuming.set(queue, consumeInfo.consumerTag)
        })
    })
  }

  unsubscribe () {
    const consuming = this.consuming
    return Promise.map(consuming.keySeq(), (queue) => {
      const consumerTag = consuming.get(queue)
      return Promise.resolve(this.channel.cancel(consumerTag))
        .then(() => {
          this.consuming = this.consuming.delete(queue)
        })
    })
  }

  disconnect () {
    if (!this.connection) {
      throw new Error('not connected. cannot disconnect.')
    }
    return Promise.resolve(this.connection.close())
  }
}

module.exports = RabbitMQ
