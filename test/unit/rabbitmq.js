'use strict'

const amqplib = require('amqplib')
const Bunyan = require('bunyan')
const chai = require('chai')
const sinon = require('sinon')

const RabbitMQ = require('../../src/rabbitmq')

const assert = chai.assert

describe('rabbitmq', () => {
  let rabbitmq
  const mockConnection = {}
  const mockChannel = {}

  beforeEach(() => {
    rabbitmq = new RabbitMQ()
    mockConnection.on = sinon.stub()
    mockConnection.createChannel = sinon.stub().resolves(mockChannel)
    mockChannel.on = sinon.stub()
  })

  describe('connect', () => {
    beforeEach(() => {
      sinon.stub(amqplib, 'connect').resolves(mockConnection)
    })

    afterEach(() => {
      amqplib.connect.restore()
    })

    it('does not connect twice', () => {
      return assert.isFulfilled(rabbitmq.connect())
        .then(() => {
          return assert.isRejected(
            rabbitmq.connect(),
            /cannot call connect twice/
          )
        })
    })

    it('does not connect twice if channel failed', () => {
      return assert.isFulfilled(rabbitmq.connect())
        .then(() => {
          delete rabbitmq.channel
          return assert.isRejected(
            rabbitmq.connect(),
            /cannot call connect twice/
          )
        })
    })

    describe('with authentication', () => {
      beforeEach(() => {
        rabbitmq.username = 'guest'
        rabbitmq.password = 'guest'
      })

      it('should connect with authentication', () => {
        return assert.isFulfilled(rabbitmq.connect())
          .then(() => {
            sinon.assert.calledOnce(amqplib.connect)
            sinon.assert.calledWithExactly(
              amqplib.connect,
              sinon.match(/guest:guest@/),
              {}
            )
          })
      })

      it('should only auth if both username and password are set', () => {
        delete rabbitmq.username
        return assert.isFulfilled(rabbitmq.connect())
          .then(() => {
            sinon.assert.calledOnce(amqplib.connect)
            sinon.assert.calledWithExactly(
              amqplib.connect,
              sinon.match(/\/\/localhost/),
              {}
            )
          })
      })
    })

    it('should reject with any connection error', () => {
      amqplib.connect.rejects(new Error('robot'))
      return assert.isRejected(
        rabbitmq.connect(),
        'robot'
      )
    })

    it('should reject with any channel creation error', () => {
      mockConnection.createChannel.rejects(new Error('robot'))
      return assert.isRejected(
        rabbitmq.connect(),
        'robot'
      )
    })

    it('should create an authentication string with hostname and port', () => {
      return assert.isFulfilled(rabbitmq.connect())
        .then(() => {
          sinon.assert.calledOnce(amqplib.connect)
          sinon.assert.calledWithExactly(
            amqplib.connect,
            sinon.match(/^amqp:\/\/guest:guest@localhost:\d+$/),
            {}
          )
        })
    })

    it('should create and save the connection', () => {
      return assert.isFulfilled(rabbitmq.connect())
        .then(() => {
          sinon.assert.calledOnce(amqplib.connect)
          assert.equal(rabbitmq.connection, mockConnection)
        })
    })

    it('appplies an error handler to the connection', () => {
      return assert.isFulfilled(rabbitmq.connect())
        .then(() => {
          sinon.assert.calledOnce(mockConnection.on)
          sinon.assert.calledWithExactly(
            mockConnection.on,
            'error',
            sinon.match.func
          )
        })
    })

    it('should create and save the channel', () => {
      return assert.isFulfilled(rabbitmq.connect())
        .then(() => {
          sinon.assert.calledOnce(mockConnection.createChannel)
          assert.equal(rabbitmq.channel, mockChannel)
        })
    })

    it('appplies an error handler to the channel', () => {
      return assert.isFulfilled(rabbitmq.connect())
        .then(() => {
          sinon.assert.calledOnce(mockChannel.on)
          sinon.assert.calledWithExactly(
            mockChannel.on,
            'error',
            sinon.match.func
          )
        })
    })
  })

  describe('_connectionErrorHandler', () => {
    beforeEach(() => {
      sinon.stub(Bunyan.prototype, 'error')
    })

    afterEach(() => {
      Bunyan.prototype.error.restore()
    })

    it('should throw the error', () => {
      assert.throws(
        () => { rabbitmq._connectionErrorHandler(new Error('foobar')) },
        'foobar'
      )
    })

    it('should log the error', () => {
      const error = new Error('foobar')
      assert.throws(
        () => { rabbitmq._connectionErrorHandler(error) },
        'foobar'
      )
      sinon.assert.calledOnce(Bunyan.prototype.error)
      sinon.assert.calledWithExactly(
        Bunyan.prototype.error,
        sinon.match.has('err', error),
        'connection has caused an error'
      )
    })
  })

  describe('_channelErrorHandler', () => {
    beforeEach(() => {
      sinon.stub(Bunyan.prototype, 'error')
    })

    afterEach(() => {
      Bunyan.prototype.error.restore()
    })

    it('should throw the error', () => {
      assert.throws(
        () => { rabbitmq._channelErrorHandler(new Error('foobar')) },
        'foobar'
      )
    })

    it('should log the error', () => {
      const error = new Error('foobar')
      assert.throws(
        () => { rabbitmq._channelErrorHandler(error) },
        'foobar'
      )
      sinon.assert.calledOnce(Bunyan.prototype.error)
      sinon.assert.calledWithExactly(
        Bunyan.prototype.error,
        sinon.match.has('err', error),
        'channel has caused an error'
      )
    })
  })

  describe('_isConnected', () => {
    it('should return true if connection and channel exist', () => {
      rabbitmq.connection = true
      rabbitmq.channel = true
      assert.ok(rabbitmq._isConnected())
    })

    it('should return false if connection or channel are missing', () => {
      rabbitmq.connection = true
      rabbitmq.channel = false
      assert.notOk(rabbitmq._isConnected())
      rabbitmq.connection = false
      rabbitmq.channel = true
      assert.notOk(rabbitmq._isConnected())
      rabbitmq.connection = false
      rabbitmq.channel = false
      assert.notOk(rabbitmq._isConnected())
    })
  })

  describe('subscribeToQueue', () => {
    const mockQueue = 'mock-queue'
    const mockHandler = () => {}

    it('should reject if rabbit has not connected', () => {
      return assert.isRejected(
        rabbitmq.subscribeToQueue(mockQueue, mockHandler),
        /must.+connect/
      )
    })

    describe('after connecting', () => {
      beforeEach(() => {
        sinon.stub(RabbitMQ.prototype, '_isConnected').returns(true)
        rabbitmq.channel = {}
        rabbitmq.channel.assertQueue = sinon.stub().resolves()
      })

      afterEach(() => {
        RabbitMQ.prototype._isConnected.restore()
      })

      it('should reject if the handler is not a function', () => {
        return assert.isRejected(
          rabbitmq.subscribeToQueue(mockQueue, 'foobar'),
          /handler.+function/
        )
      })

      it('should resolve and not create a queue if already subscribed', () => {
        return assert.isFulfilled(
          rabbitmq.subscribeToQueue(mockQueue, mockHandler)
        )
          .then(() => {
            sinon.assert.calledOnce(rabbitmq.channel.assertQueue)
            return assert.isFulfilled(
              rabbitmq.subscribeToQueue(mockQueue, mockHandler)
            )
          })
          .then(() => {
            sinon.assert.calledOnce(rabbitmq.channel.assertQueue)
          })
      })

      it('should assert a queue', () => {
        return assert.isFulfilled(
          rabbitmq.subscribeToQueue(mockQueue, mockHandler)
        )
          .then(() => {
            sinon.assert.calledOnce(rabbitmq.channel.assertQueue)
            sinon.assert.calledWithExactly(
              rabbitmq.channel.assertQueue,
              mockQueue,
              { durable: true }
            )
          })
      })

      it('should add the handler to subscriptions', () => {
        assert.equal(rabbitmq.subscriptions.size, 0)
        return assert.isFulfilled(
          rabbitmq.subscribeToQueue(mockQueue, mockHandler)
        )
          .then(() => {
            assert.equal(rabbitmq.subscriptions.size, 1)
            assert.ok(rabbitmq.subscriptions.has(mockQueue))
            assert.equal(rabbitmq.subscriptions.get(mockQueue), mockHandler)
          })
      })

      it('should add the handler to subscribed', () => {
        assert.equal(rabbitmq.subscribed.size, 0)
        return assert.isFulfilled(
          rabbitmq.subscribeToQueue(mockQueue, mockHandler)
        )
          .then(() => {
            assert.equal(rabbitmq.subscribed.size, 1)
            assert.ok(rabbitmq.subscribed.has(`queue:::${mockQueue}`))
          })
      })
    })
  })

  describe('subscribeToFanoutExchange', () => {
    const mockHandler = () => {}

    beforeEach(() => {
      sinon.stub(RabbitMQ.prototype, '_subscribeToExchange').resolves()
    })

    afterEach(() => {
      RabbitMQ.prototype._subscribeToExchange.restore()
    })

    it('should subscribe to exchange', () => {
      return assert.isFulfilled(
        rabbitmq.subscribeToFanoutExchange('exchange', mockHandler)
      )
        .then(() => {
          sinon.assert.calledOnce(RabbitMQ.prototype._subscribeToExchange)
          sinon.assert.calledWithExactly(
            RabbitMQ.prototype._subscribeToExchange,
            {
              exchange: 'exchange',
              type: 'fanout',
              handler: mockHandler
            }
          )
        })
    })
  })

  describe('subscribeToTopicExchange', () => {
    const mockHandler = () => {}

    beforeEach(() => {
      sinon.stub(RabbitMQ.prototype, '_subscribeToExchange').resolves()
    })

    afterEach(() => {
      RabbitMQ.prototype._subscribeToExchange.restore()
    })

    it('should subscribe to exchange', () => {
      return assert.isFulfilled(
        rabbitmq.subscribeToTopicExchange('exchange', 'route', mockHandler)
      )
        .then(() => {
          sinon.assert.calledOnce(RabbitMQ.prototype._subscribeToExchange)
          sinon.assert.calledWithExactly(
            RabbitMQ.prototype._subscribeToExchange,
            {
              exchange: 'exchange',
              type: 'topic',
              routingKey: 'route',
              handler: mockHandler
            }
          )
        })
    })
  })
})
