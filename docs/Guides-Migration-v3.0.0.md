# Migration Guide v3.0.0

## Breaking Changes

#### `runnable-hermes` has been removed

`runnable-hermes` has been removed from the project. This means that the constructor for the Ponos server does not take accept a `hermes` option any longer.

#### `TaskError` and `TaskFatalError` have been removed

Runnable has developed a new error library called [`ErrorCat`](https://github.com/Runnable/error-cat). `ErrorCat` provides a hierarchy of errors that create a better abstraction. `TaskError` and `TaskFatalError` have been replaced with the `WorkerError` and `WorkerStopError`, respectively, from `ErrorCat`.

#### RabbitMQ Default Authentication Cleared

The authentication parameters for RabbitMQ `username` and `password` no longer default to `guest` and `guest`. Previously, there was no way to eliminate authentication entirely. `username` and `password` are undefined by default and authentication will not be included by default when connecting to RabbitMQ.

#### Server Constructor Changed

The Ponos server constructor has been changed. First, as mentioned before, `hermes` is no longer accepted as an option: Ponos manages its own RabbitMQ library with connection controlled via a `rabbitmq` object in the server options or `RABBITMQ_*` environment variables.

Second, `queues` is no longer a required option when _not_ providing a `hermes` client. Tasks and events can be set in the constructor or after the server is created. This is detailed below.

## Migrating from v2 to v3

### Ponos Server Constructor

The server constructor has been changed to reflect the lack of `hermes` and the ability to set `task` and `event` handlers up front.

#### RabbitMQ Connection

Since a `hermes` option cannot be passed into the constructor, a `rabbitmq` option is available to set connections if that is desired. These options can also be set using `RABBITMQ_*` environment variables.

```javascript
const server = new ponos.Server({
  rabbitmq: {
    hostname: 'rabbitmq.host', // defaults to 'localhost'
    port: 5566, // defaults to 5672
    username: 'myusername', // defaults to undefined
    password: 'mypassword' // defaults to undefined
  }
})
```

#### Named Clients

`hemes` allowed the user to namespace the queues that were created when attached to a fanout exchange. To do this, now you can pass a `name` parameter to the server constructor and it will be passed to RabbitMQ.

```javascript
const server = new ponos.Server({
  name: 'my-unique-name'
})
```

#### Publishing from Workers

Projects that used `hermes` before typically used `hermes` to publish to queues as well from other workers. Ponos's RabbitMQ model is available to be required and used in a very similar way, if you wish.

```javascript
const RabbitMQ = require('ponos/lib/rabbitmq')
const rabbit = new RabbitMQ({
  // this takes the same options as the `rabbitmq` option for the server.
  // you can also set `name` here if you like, to name the client.
})

// be sure to connect to the RabbitMQ server
rabbit.connect()
  .then(() => {
    // publish directly to a queue (returns a promise)
    rabbit.publishToQueue('task.queue', { hello: 'world' })

    // or publish to an exchange (returns a promise)
    rabbit.publishToExchange('event.queue', 'routing-key', { hello: 'world' })
  })
```

#### Queues Pre-definition

`queues` are no longer required up-front when creating a server. Setting tasks and events through `setTask`, `setAllTasks`, `setEvent`, and `setAllEvents` are available, but `tasks` and `events` can be set up in the constructor now as well.

```javascript
const server = new ponos.Server({
  tasks: {
    'task.queue': require('./my-task-worker')
  },
  events: {
    'event.queue': require('./queue-worker')
  }
})
```

### Ponos Workers

#### Task (Fatal) Errors

`TaskError` and `TaskFatalError` have been removed from Ponos, in favor of errors from `ErrorCat`: `WorkerError` and `WorkerStopError`. The constructors for the errors are:

```javascript
// previously TaskError
new WorkerError('message about error', { optional: 'data' })

// previously TaskFatalError
new WorkerStopError('message about error', { optional: 'data' })
```

These constructors have almost an identical structure, but do not require the queue name as the first parameter. Ponos's worker will decorate the error with the queue name and the job for you, so you have to pass less data to these errors up front.
