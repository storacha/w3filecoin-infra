import * as Server from '@ucanto/server'

const notImplemented = () => {
  throw new Server.Failure('not implemented')
}

/**
 * @param {Partial<
 * import('@web3-storage/filecoin-client/types').AggregatorService &
 * import('@web3-storage/filecoin-client/types').DealerService
 * >} impl
 */
export function mockService(impl) {
 return {
    deal: {
      add: withCallCount(impl.deal?.add ?? notImplemented),
      queue: withCallCount(impl.deal?.queue ?? notImplemented),
    },
    aggregate: {
      add: withCallCount(impl.aggregate?.add ?? notImplemented),
      queue: withCallCount(impl.aggregate?.queue ?? notImplemented),
    },
 }
}

/**
 * @template {Function} T
 * @param {T} fn
 */
function withCallCount(fn) {
  /** @param {T extends (...args: infer A) => any ? A : never} args */
  const countedFn = (...args) => {
    countedFn.called = true
    countedFn.callCount++
    return fn(...args)
  }
  countedFn.called = false
  countedFn.callCount = 0
  return countedFn
}

