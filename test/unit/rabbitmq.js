'use strict'

const amqplib = require('amqplib')
const Bunyan = require('bunyan')
const chai = require('chai')
const clone = require('101/clone')
const Immutable = require('immutable')
const omit = require('101/omit')
const sinon = require('sinon')

const RabbitMQ = require('../../src/rabbitmq')

const assert = chai.assert

describe('rabbitmq', () => {
  let rabbitmq
  const mockConnection = {}
  const mockChannel = {}
  const mockConfirmChannel = { confirm: true }
  const prevUsername = process.env.RABBITMQ_USERNAME
  const prevPassword = process.env.RABBITMQ_PASSWORD

  beforeEach(() => {
    process.env.RABBITMQ_USERNAME = 'guest'
    process.env.RABBITMQ_PASSWORD = 'guest'
    rabbitmq = new RabbitMQ({ name: 'test-client' })
    mockConnection.on = sinon.stub()
    mockConnection.createChannel = sinon.stub().resolves(mockChannel)
    mockConnection.createConfirmChannel =
      sinon.stub().resolves(mockConfirmChannel)
    mockChannel.on = sinon.stub()
    mockConfirmChannel.on = sinon.stub()
  })

  afterEach(() => {
    process.env.RABBITMQ_USERNAME = prevUsername
    process.env.RABBITMQ_PASSWORD = prevPassword
  })

  describe('constructor', () => {
    beforeEach(() => {
      sinon.stub(Bunyan.prototype, 'warn')
    })

    afterEach(() => {
      Bunyan.prototype.warn.restore()
    })

    it('should default the name to ponos', () => {
      const r = new RabbitMQ({})
      assert.equal(r.name, 'ponos')
    })

    it('should accept a name', () => {
      const r = new RabbitMQ({ name: 'new-ponos' })
      assert.equal(r.name, 'new-ponos')
    })

    it('should accept passed in values for connection', () => {
      const r = new RabbitMQ({
        hostname: 'luke',
        port: 4242,
        username: 'myusername',
        password: 'mypassword'
      })
      assert.equal(r.hostname, 'luke')
      assert.equal(r.port, 4242)
      assert.equal(r.username, 'myusername')
      assert.equal(r.password, 'mypassword')
    })

    it('should warn if username and password not provided', () => {
      delete process.env.RABBITMQ_USERNAME
      delete process.env.RABBITMQ_PASSWORD
      const r = new RabbitMQ({})
      assert.ok(r)
      sinon.assert.calledOnce(Bunyan.prototype.warn)
      sinon.assert.calledWithExactly(
        Bunyan.prototype.warn,
        sinon.match(/username.+password/)
      )
    })

    it('should warn if password not provided', () => {
      delete process.env.RABBITMQ_USERNAME
      delete process.env.RABBITMQ_PASSWORD
      const r = new RabbitMQ({ username: 'username' })
      assert.ok(r)
      sinon.assert.calledOnce(Bunyan.prototype.warn)
      sinon.assert.calledWithExactly(
        Bunyan.prototype.warn,
        sinon.match(/username.+password/)
      )
    })
  })

  describe('connect', () => {
    beforeEach(() => {
      sinon.stub(RabbitMQ.prototype, '_isConnected').returns(false)
      sinon.stub(RabbitMQ.prototype, '_isPartlyConnected').returns(false)
      sinon.stub(amqplib, 'connect').resolves(mockConnection)
    })

    afterEach(() => {
      RabbitMQ.prototype._isConnected.restore()
      RabbitMQ.prototype._isPartlyConnected.restore()
      amqplib.connect.restore()
    })

    it('checks to see if it is connected', () => {
      return assert.isFulfilled(rabbitmq.connect())
        .then(() => {
          sinon.assert.calledOnce(RabbitMQ.prototype._isConnected)
          sinon.assert.calledOnce(RabbitMQ.prototype._isPartlyConnected)
        })
    })

    it('does not connect twice', () => {
      RabbitMQ.prototype._isConnected.returns(true)
      return assert.isRejected(
        rabbitmq.connect(),
        /cannot call connect twice/
      )
    })

    it('does not connect twice if partly connected', () => {
      RabbitMQ.prototype._isPartlyConnected.returns(true)
      return assert.isRejected(
        rabbitmq.connect(),
        /cannot call connect twice/
      )
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

    it('should reject with any channel publish creation error', () => {
      mockConnection.createConfirmChannel.rejects(new Error('luna'))
      return assert.isRejected(
        rabbitmq.connect(),
        'luna'
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

    it('should create and save a publish channel', () => {
      return assert.isFulfilled(rabbitmq.connect())
        .then(() => {
          sinon.assert.calledOnce(mockConnection.createConfirmChannel)
          assert.equal(rabbitmq.publishChannel, mockConfirmChannel)
        })
    })

    it('appplies an error handler to the publish channel', () => {
      return assert.isFulfilled(rabbitmq.connect())
        .then(() => {
          sinon.assert.calledOnce(mockConfirmChannel.on)
          sinon.assert.calledWithExactly(
            mockConfirmChannel.on,
            'error',
            sinon.match.func
          )
        })
    })
  })

  describe('publishToQueue', () => {
    const mockQueue = 'some-queue'
    const mockJob = { hello: 'world' }

    beforeEach(() => {
      rabbitmq.publishChannel = {}
      rabbitmq.publishChannel.sendToQueue = sinon.stub().resolves()
      sinon.stub(RabbitMQ.prototype, '_isConnected').returns(true)
    })

    afterEach(() => {
      RabbitMQ.prototype._isConnected.restore()
    })

    it('should reject if we are not connected', () => {
      RabbitMQ.prototype._isConnected.returns(false)
      return assert.isRejected(
        rabbitmq.publishToQueue(mockQueue, mockJob),
        /call.+connect/
      )
    })

    it('should reject if queue is not a string', () => {
      return assert.isRejected(
        rabbitmq.publishToQueue(1, mockJob),
        /queue.+string/
      )
    })

    it('should reject if queue is an empty string', () => {
      return assert.isRejected(
        rabbitmq.publishToQueue('', mockJob),
        /queue.+string/
      )
    })

    it('should reject if content is not an object', () => {
      return assert.isRejected(
        rabbitmq.publishToQueue(mockQueue, 1),
        /content.+object/
      )
    })

    it('should reject if content fails to be stringified', () => {
      sinon.stub(JSON, 'stringify').throws(new Error('custom json error'))
      return assert.isRejected(
        rabbitmq.publishToQueue(mockQueue, mockJob),
        /custom json error/
      )
        .then(() => { JSON.stringify.restore() })
    })

    it('should publish with a buffer of the content', () => {
      return assert.isFulfilled(rabbitmq.publishToQueue(mockQueue, mockJob))
        .then(() => {
          sinon.assert.calledOnce(rabbitmq.publishChannel.sendToQueue)
          sinon.assert.calledWithExactly(
            rabbitmq.publishChannel.sendToQueue,
            mockQueue,
            sinon.match.any
          )
          const contentCall = rabbitmq.publishChannel.sendToQueue.firstCall
          const content = contentCall.args.pop()
          assert.ok(Buffer.isBuffer(content))
          assert.equal(content.toString(), JSON.stringify(mockJob))
        })
    })
  })

  describe('publishToExchange', () => {
    const mockExchange = 'some-exchange'
    const mockRoutingKey = 'some-key'
    const mockJob = { hello: 'world' }

    beforeEach(() => {
      rabbitmq.publishChannel = {}
      rabbitmq.publishChannel.publish = sinon.stub().resolves()
      sinon.stub(RabbitMQ.prototype, '_isConnected').returns(true)
      sinon.stub(Bunyan.prototype, 'info')
    })

    afterEach(() => {
      RabbitMQ.prototype._isConnected.restore()
      Bunyan.prototype.info.restore()
    })

    it('should reject if we are not connected', () => {
      RabbitMQ.prototype._isConnected.returns(false)
      return assert.isRejected(
        rabbitmq.publishToExchange(mockExchange, mockRoutingKey, mockJob),
        /call.+connect/
      )
    })

    it('should reject if exchange is not a string', () => {
      return assert.isRejected(
        rabbitmq.publishToExchange(1, mockRoutingKey, mockJob),
        /exchange.+string/
      )
    })

    it('should reject if exchange is an empty string', () => {
      return assert.isRejected(
        rabbitmq.publishToExchange('', mockRoutingKey, mockJob),
        /exchange.+string/
      )
    })

    it('should reject if routingKey is not a string', () => {
      return assert.isRejected(
        rabbitmq.publishToExchange(mockExchange, 1, mockJob),
        /routingKey.+string/
      )
    })

    it('should resolve if routingKey is empty, but log info', () => {
      return assert.isFulfilled(
        rabbitmq.publishToExchange(mockExchange, '', mockJob)
      )
        .then(() => {
          sinon.assert.calledOnce(Bunyan.prototype.info)
          sinon.assert.calledWithExactly(
            Bunyan.prototype.info,
            sinon.match.object,
            sinon.match(/routingKey.+empty.+publishToQueue/)
          )
        })
    })

    it('should resolve if routingKey is empty, but still publish', () => {
      return assert.isFulfilled(
        rabbitmq.publishToExchange(mockExchange, '', mockJob)
      )
        .then(() => {
          sinon.assert.calledOnce(rabbitmq.publishChannel.publish)
        })
    })

    it('should reject if content is not an object', () => {
      return assert.isRejected(
        rabbitmq.publishToExchange(mockExchange, mockRoutingKey, 1),
        /content.+object/
      )
    })

    it('should reject if content fails to be stringified', () => {
      sinon.stub(JSON, 'stringify').throws(new Error('custom json error'))
      return assert.isRejected(
        rabbitmq.publishToExchange(mockExchange, mockRoutingKey, mockJob),
        /custom json error/
      )
        .then(() => { JSON.stringify.restore() })
    })

    it('should publish with a buffer of the content', () => {
      return assert.isFulfilled(
        rabbitmq.publishToExchange(mockExchange, mockRoutingKey, mockJob)
      )
        .then(() => {
          sinon.assert.calledOnce(rabbitmq.publishChannel.publish)
          sinon.assert.calledWithExactly(
            rabbitmq.publishChannel.publish,
            mockExchange,
            mockRoutingKey,
            sinon.match.any
          )
          const content = rabbitmq.publishChannel.publish.firstCall.args.pop()
          assert.ok(Buffer.isBuffer(content))
          assert.equal(content.toString(), JSON.stringify(mockJob))
        })
    })
  })

  describe('_connectionErrorHandler', () => {
    beforeEach(() => {
      sinon.stub(Bunyan.prototype, 'fatal')
    })

    afterEach(() => {
      Bunyan.prototype.fatal.restore()
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
      sinon.assert.calledOnce(Bunyan.prototype.fatal)
      sinon.assert.calledWithExactly(
        Bunyan.prototype.fatal,
        sinon.match.has('err', error),
        'connection has caused an error'
      )
    })
  })

  describe('_channelErrorHandler', () => {
    beforeEach(() => {
      sinon.stub(Bunyan.prototype, 'fatal')
    })

    afterEach(() => {
      Bunyan.prototype.fatal.restore()
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
      sinon.assert.calledOnce(Bunyan.prototype.fatal)
      sinon.assert.calledWithExactly(
        Bunyan.prototype.fatal,
        sinon.match.has('err', error),
        'channel has caused an error'
      )
    })
  })

  describe('_isConnected', () => {
    it('should return true if connection and channels exist', () => {
      rabbitmq._isPartlyConnected = sinon.stub().returns(true)
      rabbitmq.channel = true
      rabbitmq.publishChannel = true
      assert.ok(rabbitmq._isConnected())
    })

    it('should return false if connection or channel are missing', () => {
      rabbitmq._isPartlyConnected = sinon.stub().returns(true)
      rabbitmq.channel = false
      rabbitmq.publishChannel = true
      assert.notOk(rabbitmq._isConnected())

      rabbitmq._isPartlyConnected = sinon.stub().returns(true)
      rabbitmq.channel = true
      rabbitmq.publishChannel = false
      assert.notOk(rabbitmq._isConnected())

      rabbitmq._isPartlyConnected = sinon.stub().returns(true)
      rabbitmq.channel = false
      rabbitmq.publishChannel = false
      assert.notOk(rabbitmq._isConnected())

      rabbitmq._isPartlyConnected = sinon.stub().returns(false)
      rabbitmq.channel = false
      rabbitmq.publishChannel = true
      assert.notOk(rabbitmq._isConnected())

      rabbitmq._isPartlyConnected = sinon.stub().returns(false)
      rabbitmq.channel = true
      rabbitmq.publishChannel = false
      assert.notOk(rabbitmq._isConnected())

      rabbitmq._isPartlyConnected = sinon.stub().returns(false)
      rabbitmq.channel = false
      rabbitmq.publishChannel = false
      assert.notOk(rabbitmq._isConnected())
    })
  })

  describe('_isPartlyConnected', () => {
    it('should return true if connection exist', () => {
      rabbitmq.connection = true
      assert.ok(rabbitmq._isPartlyConnected())
    })

    it('should return false if connection is missing', () => {
      rabbitmq.connection = false
      assert.notOk(rabbitmq._isPartlyConnected())
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
              RabbitMQ.AMQPLIB_QUEUE_DEFAULTS
            )
          })
      })

      it('should assert a queue with provided options', () => {
        return assert.isFulfilled(
          rabbitmq.subscribeToQueue(
            mockQueue,
            mockHandler,
            {
              someNewOption: true,
              durable: false // override
            }
          )
        )
          .then(() => {
            const opts = clone(RabbitMQ.AMQPLIB_QUEUE_DEFAULTS)
            opts.durable = false
            opts.someNewOption = true
            sinon.assert.calledOnce(rabbitmq.channel.assertQueue)
            sinon.assert.calledWithExactly(
              rabbitmq.channel.assertQueue,
              mockQueue,
              opts
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
              handler: mockHandler,
              queueOptions: {},
              exchangeOptions: {}
            }
          )
        })
    })

    it('should accept and pass through queue options', () => {
      const fakeQueueOptions = { foobar: true }
      return assert.isFulfilled(
        rabbitmq.subscribeToFanoutExchange(
          'exchange',
          mockHandler,
          { queueOptions: fakeQueueOptions }
        )
      )
        .then(() => {
          sinon.assert.calledOnce(RabbitMQ.prototype._subscribeToExchange)
          sinon.assert.calledWithExactly(
            RabbitMQ.prototype._subscribeToExchange,
            sinon.match.has('queueOptions', fakeQueueOptions)
          )
        })
    })

    it('should accept and pass through exchange options', () => {
      const fakeExchangeOptions = { foobar: true }
      return assert.isFulfilled(
        rabbitmq.subscribeToFanoutExchange(
          'exchange',
          mockHandler,
          { exchangeOptions: fakeExchangeOptions }
        )
      )
        .then(() => {
          sinon.assert.calledOnce(RabbitMQ.prototype._subscribeToExchange)
          sinon.assert.calledWithExactly(
            RabbitMQ.prototype._subscribeToExchange,
            sinon.match.has('exchangeOptions', fakeExchangeOptions)
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
              handler: mockHandler,
              queueOptions: {},
              exchangeOptions: {}
            }
          )
        })
    })

    it('should accept and pass through queue options', () => {
      const fakeQueueOptions = { foobar: true }
      return assert.isFulfilled(
        rabbitmq.subscribeToTopicExchange(
          'exchange',
          'route',
          mockHandler,
          { queueOptions: fakeQueueOptions }
        )
      )
        .then(() => {
          sinon.assert.calledOnce(RabbitMQ.prototype._subscribeToExchange)
          sinon.assert.calledWithExactly(
            RabbitMQ.prototype._subscribeToExchange,
            sinon.match.has('queueOptions', fakeQueueOptions)
          )
        })
    })

    it('should accept and pass through exchange options', () => {
      const fakeExchangeOptions = { foobar: true }
      return assert.isFulfilled(
        rabbitmq.subscribeToTopicExchange(
          'exchange',
          'route',
          mockHandler,
          { exchangeOptions: fakeExchangeOptions }
        )
      )
        .then(() => {
          sinon.assert.calledOnce(RabbitMQ.prototype._subscribeToExchange)
          sinon.assert.calledWithExactly(
            RabbitMQ.prototype._subscribeToExchange,
            sinon.match.has('exchangeOptions', fakeExchangeOptions)
          )
        })
    })
  })

  describe('_subscribeToExchange', () => {
    const mockTopicSubscribe = {
      exchange: 'topic-exchange',
      type: 'topic',
      routingKey: 'route-key',
      handler: () => {}
    }
    const mockFanoutSubscribe = {
      exchange: 'fanout-exchange',
      type: 'fanout',
      handler: () => {}
    }

    beforeEach(() => {
      sinon.stub(rabbitmq, '_isConnected').returns(true)
      rabbitmq.channel = {}
      rabbitmq.channel.assertExchange = sinon.stub().resolves()
      rabbitmq.channel.assertQueue = sinon.stub().resolves({ queue: 'new-q' })
      rabbitmq.channel.bindQueue = sinon.stub().resolves()
    })

    it('should reject if not connected', () => {
      rabbitmq._isConnected.returns(false)
      return assert.isRejected(
        rabbitmq._subscribeToExchange(mockTopicSubscribe),
        /must.+connect/
      )
    })

    describe('fanout exchange', () => {
      it('should not subscribe if already subscribed to fanout exchange', () => {
        rabbitmq.subscribed = rabbitmq.subscribed.add('fanout:::fanout-exchange')
        return assert
        .isFulfilled(rabbitmq._subscribeToExchange(mockFanoutSubscribe))
        .then(() => {
          sinon.assert.notCalled(rabbitmq.channel.assertExchange)
        })
      })

      it('should assert the exchange', () => {
        return assert
          .isFulfilled(rabbitmq._subscribeToExchange(mockFanoutSubscribe))
          .then(() => {
            sinon.assert.calledOnce(rabbitmq.channel.assertExchange)
            sinon.assert.calledWithExactly(
              rabbitmq.channel.assertExchange,
              'fanout-exchange',
              'fanout',
              RabbitMQ.AMQPLIB_EXCHANGE_DEFAULTS
            )
          })
      })

      it('should assert an exchange with provided options', () => {
        const newOpts = clone(mockFanoutSubscribe)
        newOpts.exchangeOptions = {
          someNewOption: true,
          durable: false // override
        }
        return assert
          .isFulfilled(rabbitmq._subscribeToExchange(newOpts))
          .then(() => {
            const opts = clone(RabbitMQ.AMQPLIB_EXCHANGE_DEFAULTS)
            opts.durable = false
            opts.someNewOption = true
            sinon.assert.calledOnce(rabbitmq.channel.assertExchange)
            sinon.assert.calledWithExactly(
              rabbitmq.channel.assertExchange,
              'fanout-exchange',
              'fanout',
              opts
            )
          })
      })

      it('should create a queue for the exchange', () => {
        return assert
          .isFulfilled(rabbitmq._subscribeToExchange(mockFanoutSubscribe))
          .then(() => {
            sinon.assert.calledOnce(rabbitmq.channel.assertQueue)
            sinon.assert.calledWithExactly(
              rabbitmq.channel.assertQueue,
              'test-client.fanout-exchange',
              RabbitMQ.AMQPLIB_QUEUE_DEFAULTS
            )
          })
      })

      it('should assert a queue with provided options', () => {
        const newOpts = clone(mockFanoutSubscribe)
        newOpts.queueOptions = {
          someNewOption: true,
          durable: false // override
        }
        return assert
          .isFulfilled(rabbitmq._subscribeToExchange(newOpts))
          .then(() => {
            const opts = clone(RabbitMQ.AMQPLIB_QUEUE_DEFAULTS)
            opts.durable = false
            opts.someNewOption = true
            sinon.assert.calledOnce(rabbitmq.channel.assertQueue)
            sinon.assert.calledWithExactly(
              rabbitmq.channel.assertQueue,
              'test-client.fanout-exchange',
              opts
            )
          })
      })

      it('should bind the queue to the exchange', () => {
        return assert
          .isFulfilled(rabbitmq._subscribeToExchange(mockFanoutSubscribe))
          .then(() => {
            sinon.assert.calledOnce(rabbitmq.channel.bindQueue)
            sinon.assert.calledWithExactly(
              rabbitmq.channel.bindQueue,
              'new-q',
              'fanout-exchange',
              ''
            )
          })
      })

      it('should add the queue to the subscriptions', () => {
        assert.equal(rabbitmq.subscriptions.size, 0)
        return assert
          .isFulfilled(rabbitmq._subscribeToExchange(mockFanoutSubscribe))
          .then(() => {
            assert.equal(rabbitmq.subscriptions.size, 1)
            assert.ok(rabbitmq.subscriptions.has('new-q'))
          })
      })

      it('should add the subscribed key', () => {
        assert.equal(rabbitmq.subscribed.size, 0)
        return assert
          .isFulfilled(rabbitmq._subscribeToExchange(mockFanoutSubscribe))
          .then(() => {
            assert.equal(rabbitmq.subscribed.size, 1)
            assert.ok(rabbitmq.subscribed.has('fanout:::fanout-exchange'))
          })
      })
    })

    describe('topic exchange', () => {
      it('should assert that a topic exchange has a routing key', () => {
        const opts = omit(mockTopicSubscribe, [ 'routingKey' ])
        return assert.isRejected(
          rabbitmq._subscribeToExchange(opts),
          /routingKey.+required.+topic/
        )
      })

      it('should not subscribe if already subscribed to topic exchange', () => {
        rabbitmq.subscribed = rabbitmq.subscribed
          .add('topic:::topic-exchange:::route-key')
        return assert
          .isFulfilled(rabbitmq._subscribeToExchange(mockTopicSubscribe))
          .then(() => {
            sinon.assert.notCalled(rabbitmq.channel.assertExchange)
          })
      })

      it('should subscribe if a different topic routing key', () => {
        const opts = omit(mockTopicSubscribe, [ 'routingKey' ])
        opts.routingKey = 'route-key-dos'
        rabbitmq.subscribed = rabbitmq.subscribed
          .add('topic:::topic-exchange:::route-key')
        return assert.isFulfilled(rabbitmq._subscribeToExchange(opts))
          .then(() => {
            sinon.assert.calledOnce(rabbitmq.channel.assertExchange)
          })
      })

      it('should assert the exchange', () => {
        return assert
          .isFulfilled(rabbitmq._subscribeToExchange(mockTopicSubscribe))
          .then(() => {
            sinon.assert.calledOnce(rabbitmq.channel.assertExchange)
            sinon.assert.calledWithExactly(
              rabbitmq.channel.assertExchange,
              'topic-exchange',
              'topic',
              RabbitMQ.AMQPLIB_EXCHANGE_DEFAULTS
            )
          })
      })

      it('should assert an exchange with provided options', () => {
        const newOpts = clone(mockTopicSubscribe)
        newOpts.exchangeOptions = {
          someNewOption: true,
          durable: false // override
        }
        return assert
          .isFulfilled(rabbitmq._subscribeToExchange(newOpts))
          .then(() => {
            const opts = clone(RabbitMQ.AMQPLIB_EXCHANGE_DEFAULTS)
            opts.durable = false
            opts.someNewOption = true
            sinon.assert.calledOnce(rabbitmq.channel.assertExchange)
            sinon.assert.calledWithExactly(
              rabbitmq.channel.assertExchange,
              'topic-exchange',
              'topic',
              opts
            )
          })
      })

      it('should create a queue for the exchange', () => {
        return assert
          .isFulfilled(rabbitmq._subscribeToExchange(mockTopicSubscribe))
          .then(() => {
            sinon.assert.calledOnce(rabbitmq.channel.assertQueue)
            sinon.assert.calledWithExactly(
              rabbitmq.channel.assertQueue,
              'test-client.topic-exchange.route-key',
              RabbitMQ.AMQPLIB_QUEUE_DEFAULTS
            )
          })
      })

      it('should assert a queue with provided options', () => {
        const newOpts = clone(mockTopicSubscribe)
        newOpts.queueOptions = {
          someNewOption: true,
          durable: false // override
        }
        return assert
          .isFulfilled(rabbitmq._subscribeToExchange(newOpts))
          .then(() => {
            const opts = clone(RabbitMQ.AMQPLIB_QUEUE_DEFAULTS)
            opts.durable = false
            opts.someNewOption = true
            sinon.assert.calledOnce(rabbitmq.channel.assertQueue)
            sinon.assert.calledWithExactly(
              rabbitmq.channel.assertQueue,
              'test-client.topic-exchange.route-key',
              opts
            )
          })
      })

      it('should bind the queue to the exchange', () => {
        return assert
          .isFulfilled(rabbitmq._subscribeToExchange(mockTopicSubscribe))
          .then(() => {
            sinon.assert.calledOnce(rabbitmq.channel.bindQueue)
            sinon.assert.calledWithExactly(
              rabbitmq.channel.bindQueue,
              'new-q',
              'topic-exchange',
              'route-key'
            )
          })
      })

      it('should add the queue to the subscriptions', () => {
        assert.equal(rabbitmq.subscriptions.size, 0)
        return assert
          .isFulfilled(rabbitmq._subscribeToExchange(mockTopicSubscribe))
          .then(() => {
            assert.equal(rabbitmq.subscriptions.size, 1)
            assert.ok(rabbitmq.subscriptions.has('new-q'))
          })
      })

      it('should add the subscribed key', () => {
        assert.equal(rabbitmq.subscribed.size, 0)
        return assert
          .isFulfilled(rabbitmq._subscribeToExchange(mockTopicSubscribe))
          .then(() => {
            assert.equal(rabbitmq.subscribed.size, 1)
            assert.ok(
              rabbitmq.subscribed.has('topic:::topic-exchange:::route-key')
            )
          })
      })
    })
  })

  describe('consume', () => {
    let mockHandler

    beforeEach(() => {
      sinon.stub(rabbitmq, '_isConnected').returns(true)
      mockHandler = sinon.stub().yields() // don't yield async in this test
      rabbitmq.subscriptions = new Immutable.Map({ foo: mockHandler })
      rabbitmq.channel = {}
      rabbitmq.channel.consume = sinon.stub().resolves({ consumerTag: 'foo' })
    })

    it('should clear out the subscriptions', () => {
      assert.equal(rabbitmq.subscriptions.size, 1)
      return assert.isFulfilled(rabbitmq.consume())
        .then(() => {
          assert.equal(rabbitmq.subscriptions.size, 0)
        })
    })

    it('should reject if not connected', () => {
      rabbitmq._isConnected.returns(false)
      return assert.isRejected(rabbitmq.consume(), /must.+connect/)
    })

    it('should consume the new queues', () => {
      return assert.isFulfilled(rabbitmq.consume())
        .then(() => {
          sinon.assert.calledOnce(rabbitmq.channel.consume)
          sinon.assert.calledWithExactly(
            rabbitmq.channel.consume,
            'foo',
            sinon.match.func
          )
        })
    })

    it('should not consume queues it is already consuming', () => {
      rabbitmq.consuming = new Immutable.Set([ 'foo' ])
      return assert.isFulfilled(rabbitmq.consume())
        .then(() => {
          sinon.assert.notCalled(rabbitmq.channel.consume)
        })
    })

    it('should add the new queue to the consuming set', () => {
      assert.equal(rabbitmq.consuming.size, 0)
      return assert.isFulfilled(rabbitmq.consume())
        .then(() => {
          assert.equal(rabbitmq.consuming.size, 1)
          assert.ok(rabbitmq.consuming.has('foo'))
        })
    })

    describe('the function that is listening', () => {
      let func

      beforeEach(() => {
        rabbitmq.channel.ack = sinon.stub()
        return assert.isFulfilled(rabbitmq.consume())
          .then(() => {
            func = rabbitmq.channel.consume.firstCall.args[1]
            assert.isFunction(func)
          })
      })

      it('should call the handler with json parsed data', () => {
        func({ content: JSON.stringify({ foo: 'bar' }) })
        sinon.assert.calledOnce(mockHandler)
        sinon.assert.calledWithExactly(
          mockHandler,
          { foo: 'bar' },
          sinon.match.func
        )
      })

      it('should just acknowledge malformed jobs, not handle it', () => {
        const message = { content: new Buffer('{ nope: 1 }') }
        func(message)
        sinon.assert.calledOnce(rabbitmq.channel.ack)
        sinon.assert.calledWithExactly(rabbitmq.channel.ack, message)
        sinon.assert.notCalled(mockHandler)
      })

      it('should acknowledge the message when done', () => {
        const message = { content: JSON.stringify({ foo: 'bar' }) }
        func(message)
        sinon.assert.calledOnce(rabbitmq.channel.ack)
        sinon.assert.calledWithExactly(rabbitmq.channel.ack, message)
      })
    })
  })

  describe('unsubscribe', () => {
    beforeEach(() => {
      rabbitmq.channel = {}
      rabbitmq.channel.cancel = sinon.stub().resolves()
      rabbitmq.consuming = new Immutable.Map({ foo: 'sometag' })
    })

    it('should cancel any consuming queues', () => {
      return assert.isFulfilled(rabbitmq.unsubscribe())
        .then(() => {
          sinon.assert.calledOnce(rabbitmq.channel.cancel)
          sinon.assert.calledWithExactly(
            rabbitmq.channel.cancel,
            'sometag'
          )
        })
    })

    it('should remove the channels that were canceled', () => {
      assert.ok(rabbitmq.consuming.has('foo'))
      return assert.isFulfilled(rabbitmq.unsubscribe())
        .then(() => {
          assert.notOk(rabbitmq.consuming.has('foo'))
        })
    })

    it('should do nothing w/o any consumers', () => {
      rabbitmq.consuming = new Immutable.Map()
      return assert.isFulfilled(rabbitmq.unsubscribe())
        .then(() => {
          sinon.assert.notCalled(rabbitmq.channel.cancel)
        })
    })
  })

  describe('_setCleanState', () => {
    beforeEach(() => {
      rabbitmq.connection = {}
      rabbitmq.channel = {}
      rabbitmq.subscriptions = rabbitmq.subscriptions.set('foo', 'bar')
      rabbitmq.subscribed = rabbitmq.subscribed.add('foo')
      rabbitmq.consuming = rabbitmq.consuming.set('bar', 'foo')
    })

    it('should reset the state of the model', () => {
      assert.equal(rabbitmq.subscriptions.size, 1)
      rabbitmq._setCleanState()
      assert.notOk(rabbitmq.connection)
      assert.notOk(rabbitmq.channel)
      assert.equal(rabbitmq.subscriptions.size, 0)
      assert.equal(rabbitmq.subscribed.size, 0)
      assert.equal(rabbitmq.consuming.size, 0)
    })
  })

  describe('disconnect', () => {
    const connection = {}

    beforeEach(() => {
      sinon.stub(RabbitMQ.prototype, '_setCleanState').returns()
      rabbitmq.connection = connection
      rabbitmq.connection.close = sinon.stub().resolves()
      rabbitmq.publishChannel = {}
      rabbitmq.publishChannel.waitForConfirms = sinon.stub().resolves()
    })

    afterEach(() => {
      RabbitMQ.prototype._setCleanState.restore()
    })

    describe('when connected', () => {
      beforeEach(() => {
        sinon.stub(rabbitmq, '_isPartlyConnected').returns(true)
      })

      it('should disconnect', () => {
        return assert.isFulfilled(rabbitmq.disconnect())
          .then(() => {
            sinon.assert.calledOnce(connection.close)
          })
      })

      it('should reset the state of the model', () => {
        rabbitmq.channel = {}
        return assert.isFulfilled(rabbitmq.disconnect())
          .then(() => {
            sinon.assert.calledOnce(RabbitMQ.prototype._setCleanState)
          })
      })

      it('should wait for confirmations from the publish queue', () => {
        return assert.isFulfilled(rabbitmq.disconnect())
          .then(() => {
            sinon.assert.calledOnce(rabbitmq.publishChannel.waitForConfirms)
          })
      })
    })

    describe('when not connected', () => {
      beforeEach(() => {
        sinon.stub(rabbitmq, '_isPartlyConnected').returns(false)
      })

      it('should reject with error', () => {
        return assert.isRejected(rabbitmq.disconnect(), /not connected/)
          .then(() => {
            sinon.assert.notCalled(connection.close)
          })
      })
    })
  })
})
