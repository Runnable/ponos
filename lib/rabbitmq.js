'use strict'

const amqplib = require('amqplib')
const Immutable = require('immutable')
const Promise = require('bluebird')

const logger = require('./logger')

class RabbitMQ {
  constructor (opts) {
    this.connection = null
    this.channel = null
    this.hostname = 'localhost'
    this.port = 5672
    // this.username = 'guest'
    // this.password = 'guest'
    this.log = logger.child({
      module: 'ponos:rabbitmq',
      hostname: this.hostname,
      port: this.port
    })
    this.subscriptions = new Immutable.Map()
    this.subscribed = new Immutable.Set()
    this.consuming = new Immutable.Set()
    return this
  }

  connect () {
    if (this.connection || this.channel) {
      throw new Error('cannot call connect twice')
    }
    const url = `amqp://${this.hostname}:${this.port}`
    this.log.info({ url: url }, 'connecting')
    return amqplib.connect(url, {})
      .catch((err) => {
        this.log.error({ err: err }, 'an error occured while connecting')
        throw err
      })
      .then((conn) => {
        this.log.info('connected')
        this.connection = conn
        this.connection.on('error', this.connectionErrorHandler.bind(this))
      })
      .then(() => {
        this.log.info('creating channel')
        return this.connection.createChannel()
          .catch((err) => {
            this.log.error({ err: err }, 'an error occured creating channel')
            throw err
          })
      })
      .then((channel) => {
        this.log.info('created channel')
        this.channel = channel
        this.channel.on('error', this.channelErrorHandler.bind(this))
      })
  }

  connectionErrorHandler (err) {
    this.log.error({ err: err }, 'connection has caused an error')
    throw err
  }

  channelErrorHandler (err) {
    this.log.error({ err: err }, 'channel has caused an error')
    throw err
  }

  subscribeToTopicExchange (exchange, routingKey, handler) {
    const log = this.log.child({
      method: 'subscribeToTopicExchange',
      exchange: exchange,
      routingKey: routingKey
    })
    if (!this.channel) {
      throw Error('you must .connect() before subscribing')
    }
    if (this.subscribed.has(`${exchange}:${routingKey}`)) {
      log.info('already subscribed to topic exchange')
      return Promise.resolve()
    }
    log.info('subscribing to topic exchange')
    return this.channel.assertExchange(exchange, 'topic', { durable: false })
      .then(() => {
        log.info('exchange asserted')
        return this.channel.assertQueue('', { exclusive: true })
      })
      .then((queueInfo) => {
        const queue = queueInfo.queue
        log.info({ queue: queue }, 'queue asserted')
        log.info('binding queue')
        return this.channel.bindQueue(queue, exchange, routingKey)
          .then(() => { return queue })
      })
      .then((queue) => {
        log.info('bound queue')
        this.subscriptions = this.subscriptions.set(queue, handler)
        this.subscribed = this.subscribed.add(`${exchange}:${routingKey}`)
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
    var channel = this.channel
    subscriptions.forEach((handler, queue) => {
      log.info({ queue: queue }, 'consuming on queue')
      if (this.consuming.has(queue)) {
        log.info({ queue: queue }, 'already consuming queue')
        return true
      }
      function wrapper (msg) {
        handler(msg, () => {
          channel.ack(msg)
        })
      }
      this.channel.consume(queue, wrapper)
      this.consuming = this.consuming.add(queue)
    })
  }
}

module.exports = RabbitMQ
