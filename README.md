# Ponos

An opinionated queue based worker server for node.

For ease of use we provide options to set the host, port, username, and password to the RabbitMQ server. If not present in options, the server will attempt to use the following environment variables and final defaults:

options         | environment               | default
----------------|---------------------------|--------------
`opts.hostname` | `PONOS_RABBITMQ_HOSTNAME` | `'localhost'`
`opts.port`     | `PONOS_RABBITMQ_PORT`     | `'5672'`
`opts.username` | `PONOS_RABBITMQ_USERNAME` | `'guest'`
`opts.password` | `PONOS_RABBITMQ_PASSWORD` | `'guest'`

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
  .then(function () { console.log("Server started!"); })
  .catch(function (err) { console.error("Server failed", err); })

// Or, start using your own hermes client
var hermes = require('runnable-hermes');
var server = new ponos.Server({ hermes: hermes.hermesSingletonFactory({...}) });

// You can also nicely chain the promises!
server.setAllTasks()
  .then(server.start())
  .then(function () { /*...*/ })
  .catch(function (err) { /*...*/ });
```

## License

MIT
