/* @flow */
/* global Bluebird$Promise RabbitMQChannel RabbitMQConfirmChannel RabbitMQConnection SubscribeObject RabbitMQOptions */
'use strict'

const amqplib = require('amqplib')
const defaults = require('101/defaults')
const getNamespace = require('continuation-local-storage').getNamespace
const Immutable = require('immutable')
const isFunction = require('101/is-function')
const isObject = require('101/is-object')
const isString = require('101/is-string')
const Promise = require('bluebird')
const uuid = require('uuid')

const logger = require('./logger')

/**
 * RabbitMQ model. Can be used independently for publishing or other uses.
 *
 * @author Bryan Kendall
 * @param {Object} [opts] RabbitMQ connection options.
 * @param {Object} [opts.channel] RabbitMQ channel options.
 * @param {Object} [opts.channel.prefetch] Set prefetch for each consumer in a
 *   channel.
 * @param {String} [opts.hostname=localhost] Hostname for RabbitMQ. Can be set
 *   with environment variable RABBITMQ_HOSTNAME.
 * @param {Number} [opts.port=5672] Port for RabbitMQ. Can be set with
 *   environment variable RABBITMQ_PORT.
 * @param {String} [opts.username] Username for RabbitMQ. Can be set with
 *   environment variable RABBITMQ_USERNAME.
 * @param {String} [opts.password] Username for Password. Can be set with
 *   environment variable RABBITMQ_PASSWORD.
 */
class RabbitMQ {
  static AMQPLIB_QUEUE_DEFAULTS: Object;
  static AMQPLIB_EXCHANGE_DEFAULTS: Object;

  channel: RabbitMQChannel;
  channelOpts: Object;
  connection: RabbitMQConnection;
  consuming: Map<string, string>;
  hostname: string;
  log: Object;
  name: string;
  password: string;
  port: number;
  publishChannel: RabbitMQConfirmChannel;
  subscribed: Set<string>;
  subscriptions: Map<string, Function>;
  username: string;

  constructor (opts: Object) {
    this.name = opts.name || 'ponos'
    this.hostname = opts.hostname ||
      process.env.RABBITMQ_HOSTNAME ||
      'localhost'
    this.port = opts.port || parseInt(process.env.RABBITMQ_PORT, 10) || 5672
    this.username = opts.username || process.env.RABBITMQ_USERNAME
    this.password = opts.password || process.env.RABBITMQ_PASSWORD
    this.log = logger.child({
      module: 'ponos:rabbitmq',
      hostname: this.hostname,
      port: this.port,
      clientName: this.name
    })
    this.channelOpts = opts.channel || {}
    if (!this.username || !this.password) {
      this.log.warn(
        'RabbitMQ username and password not found. See Ponos Server ' +
        'constructor documentation.'
      )
    }
    this._setCleanState()
  }

  /**
   * Connect to the RabbitMQ server.
   *
   * @return {Promise} Promise that resolves once connection is established.
   */
  connect (): Bluebird$Promise<void> {
    if (this._isPartlyConnected() || this._isConnected()) {
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
        this.log.fatal({ err: err }, 'an error occured while connecting')
        throw err
      })
      .then((conn) => {
        this.connection = conn
        this.log.info('connected')
        this.connection.on('error', this._connectionErrorHandler.bind(this))

        this.log.info('creating channel')
        return Promise.resolve(this.connection.createChannel())
          .catch((err) => {
            this.log.fatal({ err: err }, 'an error occured creating channel')
            throw err
          })
      })
      .then((channel) => {
        if (this.channelOpts.prefetch) {
          this.log.info('setting prefetch on channel')
          return Promise.resolve(channel.prefetch(this.channelOpts.prefetch))
            .return((channel))
        }
        return channel
      })
      .then((channel) => {
        this.log.info('created channel')
        this.channel = channel
        this.channel.on('error', this._channelErrorHandler.bind(this))

        this.log.info('creating publishing channel')
        return Promise.resolve(this.connection.createConfirmChannel())
          .catch((err) => {
            this.log.fatal({ err: err }, 'errored creating confirm channel')
            throw err
          })
      })
      .then((channel) => {
        this.log.info('created confirm channel')
        this.publishChannel = channel
        this.publishChannel.on('error', this._channelErrorHandler.bind(this))
      })
  }

  /**
   * Takes an object representing a message and sends it to a queue.
   *
   * @deprecated
   * @param {String} queue Queue to receive the message.
   * @param {Object} content Content to send.
   * @return {Promise} Promise resolved when message is sent to queue.
   */
  publishToQueue (queue: string, content: Object): Bluebird$Promise<void> {
    return Promise.try(() => {
      this.log.warn({
        method: 'publishToQueue',
        queue
      }, 'rabbitmq.publishToQueue is deprecated. use `publishTask`.')
      return this.publishTask(queue, content)
    })
      .return()
  }

  /**
   * Takes an object representing a message and sends it to an exchange using
   * a provided routing key.
   *
   * Note: Providing an empty string as the routing key is functionally the same
   * as sending the message directly to a named queue. The function
   * {@link RabbitMQ#publishToQueue} is preferred in this case.
   *
   * @deprecated
   * @param {String} queue Exchange to receive the message.
   * @param {String} routingKey Routing Key for the exchange.
   * @param {Object} content Content to send.
   * @return {Promise} Promise resolved when message is sent to the exchange.
   */
  publishToExchange (
    exchange: string,
    routingKey: string,
    content: Object
  ): Bluebird$Promise<void> {
    return Promise.try(() => {
      this.log.warn({
        method: 'publishToExchange',
        exchange
      }, 'rabbitmq.publishToExchange is deprecated. use `publishEvent`.')
      return this.publishEvent(exchange, content)
    })
      .return()
  }

  /**
   * Takes an object representing a message and sends it to a task queue.
   *
   * @param {String} queue Task queue to receive the message.
   * @param {Object} content Job to send.
   * @return {Promise} Promise resolved when message is sent to queue.
   */
  publishTask (queue: string, content: Object): Bluebird$Promise<void> {
    return Promise.try(() => {
      const bufferContent = this._validatePublish(queue, content)
      return Promise.resolve(
        this.publishChannel.sendToQueue(queue, bufferContent)
      )
    })
  }

  /**
   * Sends an object representing a message to an exchange for the specified
   * event.
   *
   * @param {String} queue Exchange to receive the message.
   * @param {Object} content Content to send.
   * @return {Promise} Promise resolved when message is sent to the exchange.
   */
  publishEvent (exchange: string, content: Object): Bluebird$Promise<void> {
    return Promise.try(() => {
      const bufferContent = this._validatePublish(exchange, content)
      // events do not need a routing key (so we send '')
      return Promise.resolve(
        this.publishChannel.publish(exchange, '', bufferContent)
      )
    })
  }

  /**
   * Subscribe to a specific direct queue.
   *
   * @private
   * @param {String} queue Queue name.
   * @param {Function} handler Handler for jobs.
   * @param {Object} [queueOptions] Options for the queue.
   * @see RabbitMQ.AMQPLIB_QUEUE_DEFAULTS
   * @return {Promise} Promise that is resolved once queue is subscribed.
   */
  subscribeToQueue (
    queue: string,
    handler: Function,
    queueOptions?: Object
  ): Bluebird$Promise<void> {
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
      .resolve(
        this.channel.assertQueue(
          queue,
          defaults(queueOptions, RabbitMQ.AMQPLIB_QUEUE_DEFAULTS)
        )
      )
      .then(() => {
        log.info('queue asserted, binding queue')
        this.subscriptions = this.subscriptions.set(queue, handler)
        this.subscribed = this.subscribed.add(`queue:::${queue}`)
      })
  }

  /**
   * Subcribe to fanout exchange.
   *
   * @private
   * @param {String} exchange Name of fanout exchange.
   * @param {Function} handler Handler for jobs.
   * @param {Object} [rabbitMQOptions] Options for the queues and exchanges.
   * @param {Object} [rabbitMQOptions.queueOptions] Options for the queue.
   * @see RabbitMQ.AMQPLIB_QUEUE_DEFAULTS
   * @param {Object} [rabbitMQOptions.exchangeOptions] Options for the exchange.
   * @see RabbitMQ.AMQPLIB_EXCHANGE_DEFAULTS
   * @return {Promise} Promise resolved once subscribed.
   */
  subscribeToFanoutExchange (
    exchange: string,
    handler: Function,
    rabbitMQOptions?: RabbitMQOptions
  ): Bluebird$Promise<void> {
    const opts = {
      exchange: exchange,
      type: 'fanout',
      handler: handler,
      queueOptions: {},
      exchangeOptions: {}
    }
    if (rabbitMQOptions && rabbitMQOptions.queueOptions) {
      opts.queueOptions = rabbitMQOptions.queueOptions
    }
    if (rabbitMQOptions && rabbitMQOptions.exchangeOptions) {
      opts.exchangeOptions = rabbitMQOptions.exchangeOptions
    }
    return this._subscribeToExchange(opts)
  }

  /**
   * Subscribe to topic exchange.
   *
   * @private
   * @param {String} exchange Name of topic exchange.
   * @param {String} routingKey Routing key for topic exchange.
   * @param {Function} handler Handler for jobs.
   * @param {Object} [rabbitMQOptions] Options for the queues and exchanges.
   * @param {Object} [rabbitMQOptions.exchangeOptions] Options for the exchange.
   * @see RabbitMQ.AMQPLIB_EXCHANGE_DEFAULTS
   * @param {Object} [rabbitMQOptions.queueOptions] Options for the queue.
   * @see RabbitMQ.AMQPLIB_QUEUE_DEFAULTS
   * @return {Promise} Promise resolved once subscribed.
   */
  subscribeToTopicExchange (
    exchange: string,
    routingKey: string,
    handler: Function,
    rabbitMQOptions?: RabbitMQOptions
  ): Bluebird$Promise<void> {
    const opts = {
      exchange: exchange,
      type: 'topic',
      routingKey: routingKey,
      handler: handler,
      queueOptions: {},
      exchangeOptions: {}
    }
    if (rabbitMQOptions && rabbitMQOptions.queueOptions) {
      opts.queueOptions = rabbitMQOptions.queueOptions
    }
    if (rabbitMQOptions && rabbitMQOptions.exchangeOptions) {
      opts.exchangeOptions = rabbitMQOptions.exchangeOptions
    }
    return this._subscribeToExchange(opts)
  }

  /**
   * Start consuming from subscribed queues.
   *
   * @private
   * @return {Promise} Promise resolved when all queues consuming.
   */
  consume (): Bluebird$Promise<void> {
    const log = this.log.child({ method: 'consume' })
    log.info('starting to consume')
    if (!this._isConnected()) {
      return Promise.reject(new Error('you must .connect() before consuming'))
    }
    const subscriptions = this.subscriptions
    this.subscriptions = new Immutable.Map()
    const channel = this.channel
    return Promise.map(subscriptions.keySeq().toArray(), (queue) => {
      const handler = subscriptions.get(queue)
      log.info({ queue: queue }, 'consuming on queue')
      // XXX(bryan): is this valid? should I not be checking _this_.consuming?
      if (this.consuming.has(queue)) {
        log.warn({ queue: queue }, 'already consuming queue')
        return
      }
      function wrapper (msg) {
        let job
        try {
          job = JSON.parse(msg.content)
        } catch (err) {
          // relatively safe stringifying - could be buffer, could be invalid
          log.error({ job: '' + msg.content }, 'content not valid JSON')
          return channel.ack(msg)
        }
        handler(job, () => {
          channel.ack(msg)
        })
      }
      return Promise.resolve(this.channel.consume(queue, wrapper))
        .then((consumeInfo) => {
          this.consuming = this.consuming.set(queue, consumeInfo.consumerTag)
        })
    })
      .return()
  }

  /**
   * Unsubscribe and stop consuming from all queues.
   *
   * @private
   * @return {Promise} Promise resolved when all queues canceled.
   */
  unsubscribe (): Bluebird$Promise<void> {
    const consuming = this.consuming
    return Promise.map(consuming.keySeq().toArray(), (queue) => {
      const consumerTag = consuming.get(queue)
      return Promise.resolve(this.channel.cancel(consumerTag))
        .then(() => {
          this.consuming = this.consuming.delete(queue)
        })
    })
      .return()
  }

  /**
   * Disconnect from RabbitMQ.
   *
   * @return {Promise} Promise resolved when disconnected from RabbitMQ.
   */
  disconnect (): Bluebird$Promise<void> {
    if (!this._isPartlyConnected()) {
      return Promise.reject(new Error('not connected. cannot disconnect.'))
    }
    return Promise.resolve(this.publishChannel.waitForConfirms())
      .then(() => (Promise.resolve(this.connection.close())))
      .then(() => (this._setCleanState()))
  }

  // Private Methods

  /**
   * Helper method to re-set the state of the model to be 'clean'.
   *
   * @private
   */
  _setCleanState (): void {
    delete this.channel
    delete this.connection
    this.subscriptions = new Immutable.Map()
    this.subscribed = new Immutable.Set()
    this.consuming = new Immutable.Map()
  }

  /**
   * Error handler for the RabbitMQ connection.
   *
   * @private
   * @throws {Error}
   * @param {object} err Error object from event.
   */
  _connectionErrorHandler (err: Error) {
    this.log.fatal({ err: err }, 'connection has caused an error')
    throw err
  }

  /**
   * Error handler for the RabbitMQ channel.
   *
   * @private
   * @throws {Error}
   * @param {object} err Error object from event.
   */
  _channelErrorHandler (err: Error) {
    this.log.fatal({ err: err }, 'channel has caused an error')
    throw err
  }

  /**
   * Check to see if model is connected.
   *
   * @private
   * @return {Boolean} True if model is connected and channel is established.
   */
  _isConnected (): boolean {
    return !!(this._isPartlyConnected() && this.channel && this.publishChannel)
  }

  /**
   * Check to see if model is _partially_ connected. This means that the
   * connection was established, but the channel was not.
   *
   * @private
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
   * @param {Object} [opts.exchangeOptions] Options for the exchange.
   * @see RabbitMQ.AMQPLIB_EXCHANGE_DEFAULTS
   * @param {Object} [opts.queueOptions] Options for the queue.
   * @see RabbitMQ.AMQPLIB_QUEUE_DEFAULTS
   * @param {String} [opts.routingKey] Routing key for a topic exchange.
   * @return {Promise} Promise resolved when subcribed to exchange.
   */
  _subscribeToExchange (opts: SubscribeObject): Bluebird$Promise<void> {
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
    if (opts.type === 'topic' && opts.routingKey) {
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
          defaults(opts.exchangeOptions, RabbitMQ.AMQPLIB_EXCHANGE_DEFAULTS)
        )
      )
      .then(() => {
        log.info('exchange asserted')
        let queueName = `${this.name}.${opts.exchange}`
        if (opts.type === 'topic' && opts.routingKey) {
          queueName = `${queueName}.${opts.routingKey}`
        }
        return Promise.resolve(
          this.channel.assertQueue(
            queueName,
            defaults(opts.queueOptions, RabbitMQ.AMQPLIB_QUEUE_DEFAULTS)
          )
        )
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

  /**
   * Validate publish params. Adds a TID to the job it does not already have
   * one.
   * @private
   * @param {String} name Name of queue or exchange.
   * @param {Object} content Content to send.
   * @throws {Error} Must be connected to RabbitMQ.
   * @throws {Error} Name must be a non-empty string.
   * @throws {Error} Object must be an Object.
   * @return {Buffer} Content to send as job.
   */
  _validatePublish (name: string, content: Object): Buffer {
    if (!this._isConnected()) {
      throw new Error('you must call .connect() before publishing')
    }
    // flowtype does not prevent users from using this function incorrectly.
    if (!isString(name) || name === '') {
      throw new Error('name must be a string')
    }
    if (!isObject(content)) {
      throw new Error('content must be an object')
    }
    // add tid to message if one does not exist
    if (!content.tid) {
      const ns = getNamespace('ponos')
      const tid = ns && ns.get('tid')
      content.tid = tid || uuid()
    }
    const stringContent = JSON.stringify(content)
    return new Buffer(stringContent)
  }
}

/**
 * Default options provided for asserted queues.
 *
 * Reference the [amqplib docs]{@link
 * http://www.squaremobius.net/amqp.node/channel_api.html#channel_assertQueue}
 * for more information.
 *
 * @typedef AMQPLIB_QUEUE_DEFAULTS
 * @const {Object}
 * @property {Boolean} autoDelete=false Delete queue when it has 0 consumers.
 * @property {Boolean} durable=true Queue survives broker restarts.
 * @property {Boolean} exclusive=false Scopes the queue to the connection.
 */
RabbitMQ.AMQPLIB_QUEUE_DEFAULTS = {
  exclusive: false,
  durable: true,
  autoDelete: false
}

/**
 * Default options provided for asserted exchanges.
 *
 * Reference the [amqplib docs]{@link
 * http://www.squaremobius.net/amqp.node/channel_api.html#channel_assertExchange}
 * for more information.
 *
 * @typedef AMQPLIB_EXCHANGE_DEFAULTS
 * @const {Object}
 * @property {Boolean} autoDelete=false Delete exchange when it has 0 bindings.
 * @property {Boolean} durable=true Queue survives broker restarts.
 * @property {Boolean} internal=false Messages cannot be published directly to
 *   the exchange.
 */
RabbitMQ.AMQPLIB_EXCHANGE_DEFAULTS = {
  durable: true,
  internal: false,
  autoDelete: false
}

/**
 * RabbitMQ model.
 *
 * @module ponos/lib/rabbitmq
 * @see RabbitMQ
 */
module.exports = RabbitMQ
