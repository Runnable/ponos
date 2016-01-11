# Ponos

[![travis]](https://travis-ci.org/Runnable/ponos)
[![coveralls]](https://coveralls.io/github/Runnable/ponos?branch=master)
[![dependencies]](https://david-dm.org/Runnable/ponos)
[![devdependencies]](https://david-dm.org/Runnable/ponos#info=devDependencies)

Documentation is available at [runnable.github.io/ponos][documentation]

An opinionated queue based worker server for node.

For ease of use we provide options to set the host, port, username, and password to the RabbitMQ server. If not present in options, the server will attempt to use the following environment variables and final defaults:

options         | environment         | default
----------------|---------------------|--------------
`opts.hostname` | `RABBITMQ_HOSTNAME` | `'localhost'`
`opts.port`     | `RABBITMQ_PORT`     | `'5672'`
`opts.username` | `RABBITMQ_USERNAME` | `'guest'`
`opts.password` | `RABBITMQ_PASSWORD` | `'guest'`
`opts.log`      | *N/A*               | Basic [bunyan](https://github.com/trentm/node-bunyan) instance with `stdout` stream (for logging)
`opts.errorCat` | *N/A*               | Basic [error-cat](https://github.com/runnable/error-cat) instance (for rollbar error reporting)


Other options for Ponos are as follows:

Environment              | default | description
-------------------------|---------|------------
`WORKER_MAX_RETRY_DELAY` | `0`     | The maximum time, in milliseconds, that the worker will wait before retrying a task. The timeout will exponentially increase from `MIN_RETRY_DELAY` to `MAX_RETRY_DELAY` if the latter is set higher than the former. If this value is not set, the worker will not exponentially back off.
`WORKER_MIN_RETRY_DELAY` | `1`     | Time, in milliseconds, the worker will wait at minimum will wait before retrying a task.
`WORKER_TIMEOUT`         | `0`     | Timeout, in milliseconds, at which the worker task will be retried.


## Usage

From a high level, Ponos is used to create a worker server that responds to jobs provided from RabbitMQ. The user defines handlers for each queue's jobs that are invoked by Ponos.

Ponos has built in support for retrying and catching specific errors, which are described below.

## Workers

Workers need to be defined as a function that takes a Object `job` an returns a promise. For example:

```javascript
function myWorker (job) {
  return Promise.resolve()
    .then(function () {
      return doSomeWork(job);
    });
}
```

This worker takes the `job`, does work with it, and returns the result. Since (in theory) this does not throw any errors, the worker will see this resolution and acknowledge the job.

### Worker Errors

Ponos's worker is designed to retry any error that is not specifically a fatal error. A fatal error is defined with the `TaskFatalError` class. If a worker rejects with a `TaskFatalError`, the worker will automatically assume the job can _never_ be completed and will acknowledge the job.

As an example, a `TaskFatalError` can be used to fail a task given an invalid job:

```javascript
var TaskFatalError = require('ponos').TaskFatalError;
function myWorker (job) {
  return Promise.resolve()
    .then(function () {
      if (!job.host) { throw new TaskFatalError('host is required'); }
    })
    .then(function () {
      return doSomethingWithHost(job);
    })
    .catch(function (err) {
      myErrorReporter(err);
      throw err;
    });
}
```

This worker will reject the promise with a `TaskFatalError`. Ponos will log the error itself, acknowledge the job to remove it from the queue, and continue with other jobs. It is up to the user to additionally catch the error and log it to any other external service.

Finally, as was mentioned before, Ponos will retry any other errors. Ponos provides a `TaskError` class you may use, or you may throw normal `Error`s. If you do, the worker will catch these and retry according to the server's configuration (retry delay, back-off, max delay, etc.).

```javascript
var TaskError = require('ponos').TaskError;
var TaskFatalError = require('ponos').TaskFatalError;
function myWorker (job) {
  return Promise.resolve()
    .then(function () {
      return externalService.something(job);
    })
    // Note: w/o this catch, the error would simply propagate to the worker and
    // be handled.
    .catch(function (err) {
      logErrorToService(err);
      // If the error is 'recoverable' (e.g., network fluke, temporary outage),
      // we want to be able to retry.
      if (err.isRecoverable) {
        throw new Error('hit a momentary issue. try again.');
      } else {
        // maybe we know we can't recover from this
        throw new TaskFatalError('cannot recover. acknowledge and remove job');
      }
    });
}
```

## Worker Options
Currently workers can be defined with a `msTimeout` option. This value defaults to
`process.env.WORKER_TIMEOUT || 0`. One can set a specific millisecond timeout for
a worker like so:

```js
server.setTask('my-queue', workerFunction, { msTimeout: 1234 });
```

Or one can set this option via `setAllTasks`:

```js
server.setAllTasks({
  // This will use the default timeout...
  'queue-1': queueOneTaskFn,
  // This will use the specified timeout...
  'queue-2': {
    task: queueTwoTaskFn,
    msTimeout: 1337
  }
});
```

## Full Example

```javascript
var ponos = require('ponos');

var tasks = {
  'queue-1': function (job) { return Promise.resolve(job); },
  'queue-2': function (job) { return Promise.resolve(job); }
};

// Create the server
var server = new ponos.Server({
  queues: Object.keys(tasks);
});

// Set tasks for workers handling jobs on each queue
server.setAllTasks(tasks);

// Start the server!
server.start()
  .then(function () { console.log('Server started!'); })
  .catch(function (err) { console.error('Server failed', err); });

// Or, start using your own hermes client
var hermes = require('runnable-hermes');
var server = new ponos.Server({ hermes: hermes.hermesSingletonFactory({...}) });

// You can also nicely chain the promises!
server.start()
  .then(function () { /*...*/ })
  .catch(function (err) { /*...*/ });
```

## License

MIT

[travis]: https://img.shields.io/travis/Runnable/ponos.svg?style=flat-square "Build Status"
[coveralls]: https://img.shields.io/coveralls/Runnable/ponos/master.svg?style=flat-square "Coverage Status"
[dependencies]: https://img.shields.io/david/Runnable/ponos.svg?style=flat-square "Dependency Status"
[devdependencies]: https://img.shields.io/david/dev/Runnable/ponos.svg?style=flat-square "Dev Dependency Status"
[documentation]: https://runnable.github.io/ponos "Ponos Documentation"
