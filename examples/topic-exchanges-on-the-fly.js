const Promise = require('bluebird')

const Ponos = require('../').Server

function workerA (job) {
  return Promise.resolve()
    .then(() => {
      console.log('workerA got a job:', job)
    })
}

function workerC (job) {
  return Promise.resolve()
    .then(() => {
      console.log('workerC got a job:', job)
    })
}

function workerB (job, ponos) {
  return Promise.resolve()
    .then(() => {
      console.log('workerB got a job:', job)
      return ponos.subscribe({
        exchange: 'hello-world',
        routingKey: '#.new',
        handler: workerC
      })
        .then(() => {
          return ponos.consume()
        })
    })
}

const server = new Ponos({
  exchanges: [{
    exchange: 'hello-world',
    routingKey: '#.foo',
    handler: workerA
  }, {
    exchange: 'hello-world',
    routingKey: '#.bar',
    handler: workerB
  }]
})

server.start()
