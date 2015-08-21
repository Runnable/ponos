'use strict';

require('loadenv')('ponos:env');

var isObject = require('101/is-object');
var isString = require('101/is-string');
var exists = require('101/exists');
var defaults = require('101/defaults');
var hermes = require('runnable-hermes');
var Promise = require('bluebird');
var Worker = require('./worker');

/**
 * The ponos worker server.
 * @module ponos:server
 * @author Ryan Sandor Richards
 */
module.exports = Server;

/**
 * Ponos worker server class. Given a queue adapter the worker server will
 * connect to RabbitMQ, subscribe to the given queues, and begin spawning
 * workers for incoming jobs.
 *
 * The only required option is `opts.queues` which should be a non-empty flat
 * list of strings. The server uses this list to subscribe to only queues you
 * have provided.
 *
 * For ease of use we provide options to set the host, port, username, and
 * password to the RabbitMQ server. If not present in options, the server will
 * attempt to use the following environment variables and final defaults:
 *
 * - opts.hostname  OR  process.env.RABBITMQ_HOSTNAME  OR  'localhost'
 * - opts.port      OR  process.env.RABBITMQ_PORT      OR  '5672'
 * - opts.username  OR  process.env.RABBITMQ_USERNAME  OR  'guest'
 * - opts.password  OR  process.env.RABBITMQ_PASSWORD  OR  'guest'
 *
 * @example
 * // Create and start a new worker server
 * var ponos = require('ponos');
 * new ponos.Server({
 *   queues: ['queue-1', 'queue-2']
 * }).start()
 *   .then(function () { console.log("Server started!"); })
 *   .catch(function (err) { console.error("Server failed", err); })
 *
 * @class
 * @param {object} opts Options for the server.
 * @param {Array} opts.queues An array of queue names to which the server should
 *   subscribe.
 * @param {string} [opts.hostname] Hostname for RabbitMQ.
 * @param {string|number} [opts.port] Port for RabbitMQ.
 * @param {string} [opts.username] Username for RabbitMQ.
 * @param {string} [opts.password] Username for Password.
 */
function Server(opts) {
  this.opts = opts || {};
  defaults(this.opts, Server.DEFAULT_OPTIONS);

  if (!Array.isArray(this.opts.queues)) {
    throw new Error('ponos.Server: missing required `queues` option.');
  }

  if (!this.opts.queues.every(isString)) {
    throw new Error('ponos.Server: each element of `queues` must be a string.');
  }

  this.hermes = Promise.promisifyAll(
    hermes.hermesSingletonFactory(this.opts)
  );
};

/**
 * Default options for the worker server constructor.
 * @type {Object}
 */
Server.DEFAULT_OPTIONS = {
  hostname: process.env.RABBITMQ_HOSTNAME || 'localhost',
  port: process.env.RABBITMQ_PORT || 5672,
  username: process.env.RABBITMQ_USERNAME || 'guest',
  password: process.env.RABBITMQ_PASSWORD || 'guest'
};

/**
 * Starts the worker server and listens for jobs coming from all queues.
 * @return {Promise} A promise that resolves when the server is listening.
 */
Server.prototype.start = function () {
  var self = this;
  return this.hermes.connectAsync()
    .then(Promise.map(self.hermes.queues, self._subscribe));
};

/**
 * Stops the worker server.
 * @return {Promise} A promise that resolves when the server is stopped.
 */
Server.prototype.stop = function () {
  return this.hermes.closeAsync();
};

/**
 * Helper method that subscribes to a given queue name.
 * @param  {string} queueName Name of the queue.
 * @return {Promise}
 */
Server.prototype._subscribe = function (queueName) {
  return this.hermes.subscribeAsync(queueName, function (job, done) {
    Worker.create(name, job, done);
  });
};
