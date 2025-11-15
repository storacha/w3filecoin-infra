import anyTest from 'ava'
import dotenv from 'dotenv'

dotenv.config({
  path: '.env.local'
})

/**
 * @typedef {'aggregator' | 'dealer' | 'tracker'} ServiceName
 *
 * @typedef {object} AggregatorStores
 * @property {import('@storacha/filecoin-api/aggregator/api').PieceStore} pieceStore
 * @property {import('@storacha/filecoin-api/aggregator/api').InclusionStore} inclusionStore
 * @property {import('@w3filecoin/core/src/store/types.js').CustomAggregateStore} aggregateStore
 * @property {import('@storacha/filecoin-api/aggregator/api').BufferStore} bufferStore
 * @property {import('@w3filecoin/core/src/store/types.js').InclusionProofStore} inclusionProofStore
 * @typedef {object} DealerStores
 * @property {import('@storacha/filecoin-api/dealer/api').AggregateStore} aggregateStore
 * @property {import('@storacha/filecoin-api/dealer/api').OfferStore<any>} offerStore
 * @typedef {object} TrackerStores
 * @property {import('@storacha/filecoin-api/deal-tracker/api').DealStore} dealStore
 * @typedef {object} StorefrontStores
 * @property {import('@storacha/filecoin-api/storefront/api').ReceiptStore} receiptStore
 *
 * @typedef {object} Stores
 * @property {AggregatorStores} aggregator
 * @property {DealerStores} dealer
 * @property {TrackerStores} tracker
 * @property {StorefrontStores} storefront
 *
 * @typedef {object} Context
 * @property {Record<ServiceName, string>} api
 * @property {Stores} store
 *
 * @typedef {import('ava').TestFn<Awaited<Context>>} TestContextFn
 */

// eslint-disable-next-line unicorn/prefer-export-from
export const test = /** @type {TestContextFn} */ (anyTest)
