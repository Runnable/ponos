'use strict';

require('loadenv')('ponos:env');

var assign = require('101/assign');
var defaults = require('101/defaults');
var ErrorCat = require('error-cat');
var hermes = require('runnable-hermes');
var isString = require('101/is-string');
var log = require('./logger');
var Promise = require('bluebird');
var Worker = require('./worker');

/**
 * The ponos worker server.
 * @module ponos:server
 * @author Bryan Kendall
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
 * @class
 * @param {object} opts Options for the server.
 * @param {Array} opts.queues An array of queue names to which the server should
 *   subscribe.
 * @param {runnable-hermes~Hermes} [opts.hermes] A hermes client.
 * @param {string} [opts.hostname] Hostname for RabbitMQ.
 * @param {string|number} [opts.port] Port for RabbitMQ.
 * @param {string} [opts.username] Username for RabbitMQ.
 * @param {string} [opts.password] Username for Password.
 * @param {bunyan} [opts.log] A bunyan logger to use for the server.
 * @param {ErrorCat} [opts.errorCat] An error cat instance to use for the
 *   server.
 */
function Server (opts) {
  this._tasks = {};
  this.opts = assign({}, opts);

  this.log = this.opts.log || log;
  this.errorCat = this.opts.errorCat || new ErrorCat();

  if (this.opts.hermes) {
    this.hermes = this.opts.hermes;
  } else {
    if (!Array.isArray(this.opts.queues)) {
      throw new Error('ponos.Server: missing required `queues` option.');
    }
    if (!this.opts.queues.every(isString)) {
      throw new Error(
        'ponos.Server: each element of `queues` must be a string.'
      );
    }
    // Defaults for the hermes client's rabbitmq connection
    defaults(this.opts, {
      hostname: process.env.RABBITMQ_HOSTNAME || 'localhost',
      port: process.env.RABBITMQ_PORT || 5672,
      username: process.env.RABBITMQ_USERNAME || 'guest',
      password: process.env.RABBITMQ_PASSWORD || 'guest'
    });

    this.hermes = hermes.hermesSingletonFactory(this.opts);
  }

  this.hermes = Promise.promisifyAll(this.hermes);
};

/**
 * Starts the worker server and listens for jobs coming from all queues.
 * @return {promise} A promise that resolves when the server is listening.
 */
Server.prototype.start = function () {
  this.log.trace('Starting ponos worker server');
  return this.hermes.connectAsync().bind(this)
    .then(this._assertHaveAllTasks)
    .then(this._subscribeAll)
    .then(function () {
      this.log.trace('Ponos worker server started');
    })
    .catch(function (err) {
      this.errorCat.report(err);
      throw err;
    });
};

/**
 * Helper function to ensure we have all required task handlers for the queue
 * array with which the class was defined.
 * @return {promise} Resolved when all tasks verified.
 */
Server.prototype._assertHaveAllTasks = function () {
  return Promise.resolve(this.hermes.queues).bind(this)
    .each(function (queueName) {
      if (!this._tasks[queueName]) {
        throw new Error(queueName + ' handler not defined');
      }
    });
};

/**
 * Stops the worker server.
 * @return {promise} A promise that resolves when the server is stopped.
 */
Server.prototype.stop = function () {
  this.log.trace('Ponos server stopping');
  return this.hermes.closeAsync()
    .then(function () {
      this.log.trace('Ponos server stopped');
    }.bind(this))
    .catch(function (err) {
      this.errorCat.report(err);
      throw err;
    });
};

/**
 * Takes a map of queues and task handlers and sets them all.
 * @param {object} map A map of queue names to task handlers.
 * @param {string} map.key Queue name.
 * @param {function} map.value Function to take a job and return a promise
 * @returns {promise} Resolved when all tasks are assigned to queues.
 */
Server.prototype.setAllTasks = function (map) {
  return Promise.resolve(Object.keys(map)).bind(this)
    .each(function (key) { return this.setTask(key, map[key]); });
};

/**
 * Assigns a task to a queue.
 * @param {string} queueName Queue name.
 * @param {function} task Function to take a job and return a promise.
 * @returns {promise} Resolved when task assigned to queue.
 */
Server.prototype.setTask = function (queueName, task) {
  this.log.trace('setting task for ' + queueName);
  return Promise.resolve().bind(this)
    .then(function () { this._tasks[queueName] = task; });
};

/**
 * Helper function to subscribe to all queues.
 * @return {promise} Resolved when queues are all subscribed.
 */
Server.prototype._subscribeAll = function () {
  return Promise.resolve(this.hermes.queues).bind(this)
    .map(this._subscribe);
};

/**
 * Helper function that subscribes to a given queue name.
 * @param {string} queueName Name of the queue.
 * @return {promise} A promise that resolves post worker creation
 *   and subscription
 */
Server.prototype._subscribe = function (queueName) {
  this.log.trace('subscribing to ' + queueName);
  return this.hermes.subscribeAsync(queueName, function (job, done) {
    Worker.create({
      queue: queueName,
      job: job,
      task: this._tasks[queueName],
      done: done,
      log: this.log,
      errorCat: this.errorCat
    });
  }.bind(this));
};
