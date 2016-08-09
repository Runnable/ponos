'use strict'

const EventEmitter = require('events')
const Promise = require('bluebird')
const getNamespace = require('continuation-local-storage').getNamespace

const testTid = (step) => {
  if (typeof getNamespace('ponos').get('tid') !== 'string') {
    throw new Error('tid not found after Promise.' + step)
  }
}

/**
 * A simple worker that will publish a message to a queue.
 * @param {object} job Object describing the job.
 * @param {string} job.queue Queue on which the message will be published.
 * @returns {promise} Resolved when the message is put on the queue.
 */
module.exports = (job) => {
  return Promise
    .try(() => {
      testTid('try')
      return Promise.try(() => {
        testTid('try.try')
      }).then(() => {
        testTid('try.then')
      })
      .then(() => {
        return Promise.resolve()
          .then(() => {
            testTid('try.then.resolve.then')
          })
      })
    })
    .then(() => {
      testTid('then')
      return [1, 2, 3]
    })
    .spread((a, b, c) => {
      testTid('spread')
      return [1, 2, 3]
    })
    .then(() => {
      testTid('then after spread')
      throw new Error('test')
    })
    .catch(() => {
      testTid('catch')
    })
    .then(() => {
      testTid('then after catch')
    })
    .finally(() => {
      testTid('finally')
    })
    .then(() => {
      testTid('then after finally')
    })
    .then(() => {
      return Promise.resolve().then(() => {
        testTid('then.resolve.then.bind')
      })
      .bind(this)
      .then(() => {
        testTid('then.resolve.then.bind.then')
      })
    })
    .then(() => {
      return Promise.reject(new Error('test')).catch(() => {
        testTid('then.resolve.reject.catch')
      })
    })
    .then(() => {
      return Promise.all([
        Promise.resolve().then(() => { testTid('then.all.resolve.then') }),
        Promise.try(() => { testTid('then.all.try') })
      ])
      .then(() => {
        testTid('then after all')
      })
    })
    .then(() => {
      return Promise.props({
        a: Promise.resolve().then(() => { testTid('then.props.resolve.then') }),
        b: Promise.try(() => { testTid('then.props.try') })
      })
      .then(() => {
        testTid('then after props')
      })
    })
    .then(() => {
      return Promise.any([
        Promise.resolve().then(() => { testTid('then.any.resolve.then') }),
        Promise.try(() => { testTid('then.any.try') })
      ])
      .then(() => {
        testTid('then after any')
      })
    })
    .then(() => {
      return Promise.some([
        Promise.resolve().then(() => { testTid('then.some.resolve.then') }),
        Promise.try(() => { testTid('then.some.try') })
      ], 2)
      .then(() => {
        testTid('then after some')
      })
    })
    .then(() => {
      return Promise.map([1, 2], () => {
        testTid('then.map')
      })
      .then(() => {
        testTid('then after map')
      })
    })
    .then(() => {
      return Promise.reduce([1, 2], () => {
        testTid('then.reduce')
      })
      .then(() => {
        testTid('then after reduce')
      })
    })
    .then(() => {
      return Promise.filter([1, 2], () => {
        testTid('then.filter')
      })
      .then(() => {
        testTid('then after filter')
      })
    })
    .then(() => {
      return Promise.each([1, 2], () => {
        testTid('then.each')
      })
      .then(() => {
        testTid('then after each')
      })
    })
    .then(() => {
      return Promise.mapSeries([1, 2], () => {
        testTid('then.mapSeries')
      })
      .then(() => {
        testTid('then after mapSeries')
      })
    })
    .then(() => {
      return Promise.race([
        Promise.resolve().then(() => { testTid('then.race.resolve.then') }),
        Promise.try(() => { testTid('then.race.try') })
      ])
      .then(() => {
        testTid('then after race')
      })
    })
    .then(() => {
      return Promise.using(() => {
        return Promise.resolve().disposer(() => {
          testTid('then.using.resolve.disposer')
        })
      }, () => {
        return Promise.try(() => { testTid('then.using.try') })
      })
      .then(() => {
        testTid('then after using')
      })
    })
    .then(() => {
      const testFuncs = {
        sync: (cb) => {
          testTid('promisify.sync')
          cb()
        },
        cb: (cb) => {
          testTid('promisify.cb')
          setTimeout(() => {
            testTid('promisify.cb.setTimeout')
            cb()
          })
        }
      }
      const syncA = Promise.promisify(testFuncs.sync)
      const cbA = Promise.promisify(testFuncs.cb)
      return Promise.all([syncA(), cbA()])
        .then(() => {
          testTid('then.promisify.all.then')
        })
    })
    .then(() => {
      const testFuncs = {
        sync: (cb) => {
          testTid('promisifyAll.sync')
          cb()
        },
        cb: (cb) => {
          testTid('promisifyAll.cb')
          setTimeout(() => {
            testTid('promisifyAll.cb.setTimeout')
            cb()
          }, 10)
        }
      }
      Promise.promisifyAll(testFuncs)
      return Promise.all([testFuncs.syncAsync(), testFuncs.cbAsync()])
        .then(() => {
          testTid('then.promisifyAll.all.then')
        })
    })
    .then(() => {
      const testFuncs = {
        sync: (cb) => {
          testTid('fromCallback.sync')
          cb()
        },
        cb: (cb) => {
          testTid('fromCallback.cb')
          setTimeout(() => {
            testTid('fromCallback.cb.setTimeout')
            cb()
          })
        }
      }
      return Promise.all([Promise.fromCallback((cb) => {
        testTid('then.all.fromCallback.sync')
        testFuncs.sync(cb)
      }), Promise.fromCallback((cb) => {
        testTid('then.all.fromCallback.cb')
        testFuncs.cb(cb)
      })])
      .then(() => {
        testTid('then.fromCallback.all.then')
      })
    })
    .then(() => {
      return Promise.fromCallback((cb) => {
        testTid('then.fromCallback')
        return Promise.resolve().asCallback(() => {
          testTid('then.asCallback')
          cb()
        })
      })
      .then(() => {
        testTid('then after asCallback')
      })
    })
    .then(() => {
      return Promise.delay(1)
        .then(() => {
          testTid('then after delay')
        })
    })
    .then(() => {
      return Promise.delay(100).timeout(10).catch(Promise.TimeoutError, () => {
        testTid('then.TimeoutError')
      })
      .then(() => {
        testTid('then after timeout')
      })
    })
    .tap(() => {
      testTid('tap')
    })
    .then(() => {
      module.exports.emitter.emit('passed')
    })
    .catch((err) => {
      module.exports.emitter.emit('failed', err)
    })
}

module.exports.emitter = new EventEmitter()
