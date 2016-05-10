/* @flow */
/* global RabbitMQChannel RabbitMQConnection SubscribeObject */
'use strict'

const amqplib = require('amqplib')
const Immutable = require('immutable')
const isFunction = require('101/is-function')
const Promise = require('bluebird')

const logger = require('./logger')

/**
 * RabbitMQ model. Takes authentication from the following environment
 * variables:
 * - RABBITMQ_HOSTNAME (default: 'localhost')
 * - RABBITMQ_PORT (default: 5672)
 * - RABBITMQ_USERNAME (no default)
 * - RABBITMQ_PASSWORD (no default)
 *
 * @private
 * @author Bryan Kendall
 */
class RabbitMQ {
  channel: RabbitMQChannel;
  connection: RabbitMQConnection;
  consuming: Map<string, string>;
  hostname: string;
  log: Object;
  password: string;
  port: number;
  subscribed: Set<string>;
  subscriptions: Map<string, Function>;
  username: string;

  constructor () {
    this.hostname = process.env.RABBITMQ_HOSTNAME || 'localhost'
    this.port = parseInt(process.env.RABBITMQ_PORT, 10) || 5672
    this.username = `${process.env.RABBITMQ_USERNAME}`
    this.password = `${process.env.RABBITMQ_PASSWORD}`
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
   *
   * @return {Promise} Promise that resolves once connection is established.
   */
  connect (): Promise {
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
   * Subscribe to a specific direct queue.
   *
   * @param {String} queue Queue name.
   * @param {Function} handler Handler for jobs.
   * @return {Promise} Promise that is resolved once queue is subscribed.
   */
  subscribeToQueue (queue: string, handler: Function): Promise {
    const log = this.log.child({
      method: 'subscribeToQueue',
      queue: queue
    })
    log.info('subscribing to queue')
    if (!this._isConnected()) {
      return Promise.reject(new Error('you must .connect() before subscribing'))
    }
    if (!isFunction(handler)) {
      log.error('handler must be a function')
      return Promise.reject(
        new Error(`handler for ${queue} must be a function`)
      )
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

  /**
   * Subcribe to fanout exchange.
   *
   * @param {String} exchange Name of fanout exchange.
   * @param {Function} handler Handler for jobs.
   * @return {Promise} Promise resolved once subscribed.
   */
  subscribeToFanoutExchange (exchange: string, handler: Function): Promise {
    return this._subscribeToExchange({
      exchange: exchange,
      type: 'fanout',
      handler: handler
    })
  }

  /**
   * Subscribe to topic exchange.
   *
   * @param {String} exchange Name of topic exchange.
   * @param {String} routingKey Routing key for topic exchange.
   * @param {Function} handler Handler for jobs.
   * @return {Promise} Promise resolved once subscribed.
   */
  subscribeToTopicExchange (
    exchange: string,
    routingKey: string,
    handler: Function
  ): Promise {
    return this._subscribeToExchange({
      exchange: exchange,
      type: 'topic',
      routingKey: routingKey,
      handler: handler
    })
  }

  /**
   * Start consuming from subscribed queues.
   *
   * @return {Promise} Promise resolved when all queues consuming.
   */
  consume (): Promise {
    const log = this.log.child({ method: 'consume' })
    log.info('starting to consume')
    if (!this._isConnected()) {
      return Promise.reject(new Error('you must .connect() before consuming'))
    }
    const subscriptions = this.subscriptions
    this.subscriptions = new Immutable.Map()
    const channel = this.channel
    return Promise.map(subscriptions.keySeq(), (queue) => {
      const handler = subscriptions.get(queue)
      log.info({ queue: queue }, 'consuming on queue')
      // XXX(bryan): is this valid? should I not be checking _this_.consuming?
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

  /**
   * Unsubscribe and stop consuming from all queues.
   *
   * @return {Promise} Promise resolved when all queues canceled.
   */
  unsubscribe (): Promise {
    const consuming = this.consuming
    return Promise.map(consuming.keySeq(), (queue) => {
      const consumerTag = consuming.get(queue)
      return Promise.resolve(this.channel.cancel(consumerTag))
        .then(() => {
          this.consuming = this.consuming.delete(queue)
        })
    })
  }

  /**
   * Disconnect from RabbitMQ.
   *
   * @return {Promise} Promise resolved when disconnected from RabbitMQ.
   */
  disconnect (): Promise {
    if (!this._isPartlyConnected()) {
      return Promise.reject(new Error('not connected. cannot disconnect.'))
    }
    return Promise.resolve(this.connection.close())
  }

  // Private Methods

  /**
   * Error handler for the RabbitMQ connection.
   *
   * @private
   * @throws Error
   * @param {object} err Error object from event.
   */
  _connectionErrorHandler (err: Error) {
    this.log.error({ err: err }, 'connection has caused an error')
    throw err
  }

  /**
   * Error handler for the RabbitMQ channel.
   * @private
   * @throws Error
   * @param {object} err Error object from event.
   */
  _channelErrorHandler (err: Error) {
    this.log.error({ err: err }, 'channel has caused an error')
    throw err
  }

  /**
   * Check to see if model is connected.
   *
   * @return {Boolean} True if model is connected and channel is established.
   */
  _isConnected (): boolean {
    return !!(this._isPartlyConnected() && this.channel)
  }

  /**
   * Check to see if model is _partially_ connected. This means that the
   * connection was established, but the channel was not.
   *
   * @return {Boolean} True if connection is established.
   */
  _isPartlyConnected (): boolean {
    return !!(this.connection)
  }

  /**
   * Helper function to consolidate logic for subscribing to queues. Stores
   * information about what is subscribed and is responsible for asserting
   * exchanges and queues into existance.
   *
   * @private
   * @param {Object} opts Object describing the exchange connection.
   * @param {String} opts.exchange Name of exchange.
   * @param {String} opts.handler Handler of jobs.
   * @param {String} opts.type Type of exchange: 'fanout' or 'topic'.
   * @param {String} [opts.routingKey] Routing key for a topic exchange.
   * @return {Promise} Promise resolved when subcribed to exchange.
   */
  _subscribeToExchange (opts: SubscribeObject): Promise {
    const log = this.log.child({
      method: '_subscribeToExchange',
      opts: opts
    })
    log.info('subscribing to exchange')
    if (!this._isConnected()) {
      return Promise.reject(new Error('must .connect() before subscribing'))
    }
    if (opts.type === 'topic' && !opts.routingKey) {
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
}

/**
 * RabbitMQ model.
 * @private
 * @module ponos/lib/rabbitmq
 * @see RabbitMQ
 */
module.exports = RabbitMQ
