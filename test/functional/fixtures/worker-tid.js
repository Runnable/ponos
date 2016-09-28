'use strict'

const EventEmitter = require('events')
const Promise = require('bluebird')
const getNamespace = require('continuation-local-storage').getNamespace

const testClsData = (step) => {
  if (typeof getNamespace('ponos').get('tid') !== 'string') {
    throw new Error('tid not found after Promise.' + step)
  }
  if (getNamespace('ponos').get('previousEvent') !== 'ponos-test:one') {
    throw new Error('previousEvent not found after Promise.' + step)
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
      testClsData('try')
      return Promise.try(() => {
        testClsData('try.try')
      }).then(() => {
        testClsData('try.then')
      })
      .then(() => {
        return Promise.resolve()
          .then(() => {
            testClsData('try.then.resolve.then')
          })
      })
    })
    .then(() => {
      testClsData('then')
      return [1, 2, 3]
    })
    .spread((a, b, c) => {
      testClsData('spread')
      return [1, 2, 3]
    })
    .then(() => {
      testClsData('then after spread')
      throw new Error('test')
    })
    .catch(() => {
      testClsData('catch')
    })
    .then(() => {
      testClsData('then after catch')
    })
    .finally(() => {
      testClsData('finally')
    })
    .then(() => {
      testClsData('then after finally')
    })
    .then(() => {
      return Promise.resolve().then(() => {
        testClsData('then.resolve.then.bind')
      })
      .bind(this)
      .then(() => {
        testClsData('then.resolve.then.bind.then')
      })
    })
    .then(() => {
      return Promise.reject(new Error('test')).catch(() => {
        testClsData('then.resolve.reject.catch')
      })
    })
    .then(() => {
      return Promise.all([
        Promise.resolve().then(() => { testClsData('then.all.resolve.then') }),
        Promise.try(() => { testClsData('then.all.try') })
      ])
      .then(() => {
        testClsData('then after all')
      })
    })
    .then(() => {
      return Promise.props({
        a: Promise.resolve().then(() => { testClsData('then.props.resolve.then') }),
        b: Promise.try(() => { testClsData('then.props.try') })
      })
      .then(() => {
        testClsData('then after props')
      })
    })
    .then(() => {
      return Promise.any([
        Promise.resolve().then(() => { testClsData('then.any.resolve.then') }),
        Promise.try(() => { testClsData('then.any.try') })
      ])
      .then(() => {
        testClsData('then after any')
      })
    })
    .then(() => {
      return Promise.some([
        Promise.resolve().then(() => { testClsData('then.some.resolve.then') }),
        Promise.try(() => { testClsData('then.some.try') })
      ], 2)
      .then(() => {
        testClsData('then after some')
      })
    })
    .then(() => {
      return Promise.map([1, 2], () => {
        testClsData('then.map')
      })
      .then(() => {
        testClsData('then after map')
      })
    })
    .then(() => {
      return Promise.reduce([1, 2], () => {
        testClsData('then.reduce')
      })
      .then(() => {
        testClsData('then after reduce')
      })
    })
    .then(() => {
      return Promise.filter([1, 2], () => {
        testClsData('then.filter')
      })
      .then(() => {
        testClsData('then after filter')
      })
    })
    .then(() => {
      return Promise.each([1, 2], () => {
        testClsData('then.each')
      })
      .then(() => {
        testClsData('then after each')
      })
    })
    .then(() => {
      return Promise.mapSeries([1, 2], () => {
        testClsData('then.mapSeries')
      })
      .then(() => {
        testClsData('then after mapSeries')
      })
    })
    .then(() => {
      return Promise.race([
        Promise.resolve().then(() => { testClsData('then.race.resolve.then') }),
        Promise.try(() => { testClsData('then.race.try') })
      ])
      .then(() => {
        testClsData('then after race')
      })
    })
    .then(() => {
      return Promise.using(() => {
        return Promise.resolve().disposer(() => {
          testClsData('then.using.resolve.disposer')
        })
      }, () => {
        return Promise.try(() => { testClsData('then.using.try') })
      })
      .then(() => {
        testClsData('then after using')
      })
    })
    .then(() => {
      const testFuncs = {
        sync: (cb) => {
          testClsData('promisify.sync')
          cb()
        },
        cb: (cb) => {
          testClsData('promisify.cb')
          setTimeout(() => {
            testClsData('promisify.cb.setTimeout')
            cb()
          })
        }
      }
      const syncA = Promise.promisify(testFuncs.sync)
      const cbA = Promise.promisify(testFuncs.cb)
      return Promise.all([syncA(), cbA()])
        .then(() => {
          testClsData('then.promisify.all.then')
        })
    })
    .then(() => {
      const testFuncs = {
        sync: (cb) => {
          testClsData('promisifyAll.sync')
          cb()
        },
        cb: (cb) => {
          testClsData('promisifyAll.cb')
          setTimeout(() => {
            testClsData('promisifyAll.cb.setTimeout')
            cb()
          }, 10)
        }
      }
      Promise.promisifyAll(testFuncs)
      return Promise.all([testFuncs.syncAsync(), testFuncs.cbAsync()])
        .then(() => {
          testClsData('then.promisifyAll.all.then')
        })
    })
    .then(() => {
      const testFuncs = {
        sync: (cb) => {
          testClsData('fromCallback.sync')
          cb()
        },
        cb: (cb) => {
          testClsData('fromCallback.cb')
          setTimeout(() => {
            testClsData('fromCallback.cb.setTimeout')
            cb()
          })
        }
      }
      return Promise.all([Promise.fromCallback((cb) => {
        testClsData('then.all.fromCallback.sync')
        testFuncs.sync(cb)
      }), Promise.fromCallback((cb) => {
        testClsData('then.all.fromCallback.cb')
        testFuncs.cb(cb)
      })])
      .then(() => {
        testClsData('then.fromCallback.all.then')
      })
    })
    .then(() => {
      return Promise.fromCallback((cb) => {
        testClsData('then.fromCallback')
        return Promise.resolve().asCallback(() => {
          testClsData('then.asCallback')
          cb()
        })
      })
      .then(() => {
        testClsData('then after asCallback')
      })
    })
    .then(() => {
      return Promise.delay(1)
        .then(() => {
          testClsData('then after delay')
        })
    })
    .then(() => {
      return Promise.delay(100).timeout(10).catch(Promise.TimeoutError, () => {
        testClsData('then.TimeoutError')
      })
      .then(() => {
        testClsData('then after timeout')
      })
    })
    .tap(() => {
      testClsData('tap')
    })
    .then(() => {
      module.exports.emitter.emit('passed')
    })
    .catch((err) => {
      module.exports.emitter.emit('failed', err)
    })
}

module.exports.emitter = new EventEmitter()
