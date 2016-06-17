'use strict'

const getNamespace = require('continuation-local-storage').getNamespace
const Promise = require('bluebird')

const Ponos = require('../')

function workerA (job) {
  return Promise.try(() => {
    const tid = getNamespace('ponos').get('tid')
    console.log('workerA got a job:', job, 'tid:', tid)
  })
}

function workerC (job) {
  return Promise.try(() => {
    const tid = getNamespace('ponos').get('tid')
    console.log('workerC got a job:', job, 'tid:', tid)
  })
}

function workerB (job, ponos) {
  return Promise.try(() => {
    const tid = getNamespace('ponos').get('tid')
    console.log('workerB got a job:', job, 'tid:', tid)
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

const server = new Ponos.Server({
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
