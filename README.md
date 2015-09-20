# Ponos

An opinionated queue based worker server for node.

For ease of use we provide options to set the host, port, username, and password to the RabbitMQ server. If not present in options, the server will attempt to use the following environment variables and final defaults:

options         | environment               | default
----------------|---------------------------|--------------
`opts.hostname` | `PONOS_RABBITMQ_HOSTNAME` | `'localhost'`
`opts.port`     | `PONOS_RABBITMQ_PORT`     | `'5672'`
`opts.username` | `PONOS_RABBITMQ_USERNAME` | `'guest'`
`opts.password` | `PONOS_RABBITMQ_PASSWORD` | `'guest'`

### Usage

From a high level, Ponos is used to create a worker server that responds to jobs provided from RabbitMQ. The user defines handlers for each queue's jobs that are invoked by Ponos.

Ponos has built in support for retrying and catching specific errors, which are described below.

### Workers

// TODO(bryan)

### Example

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
