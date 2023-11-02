import anyTest from 'ava'
import dotenv from 'dotenv'

dotenv.config({
  path: '.env.local'
})

/**
 * @typedef {'aggregator' | 'dealer' | 'tracker'} ServiceName
 * 
 * @typedef {object} AggregatorStores
 * @property {import('@web3-storage/filecoin-api/aggregator/api').PieceStore} pieceStore
 * @property {import('@web3-storage/filecoin-api/aggregator/api').InclusionStore} inclusionStore
 * @property {import('@w3filecoin/core/src/store/types').CustomAggregateStore} aggregateStore
 * @property {import('@web3-storage/filecoin-api/aggregator/api').BufferStore} bufferStore
 * @property {import('@w3filecoin/core/src/store/types').InclusionProofStore} inclusionProofStore
 * @typedef {object} DealerStores
 * @property {import('@web3-storage/filecoin-api/dealer/api').AggregateStore} aggregateStore
 * @property {import('@web3-storage/filecoin-api/dealer/api').OfferStore<any>} offerStore
 * @typedef {object} TrackerStores
 * @property {import('@web3-storage/filecoin-api/deal-tracker/api').DealStore} dealStore
 * @typedef {object} StorefrontStores
 * @property {import('@web3-storage/filecoin-api/storefront/api').ReceiptStore} receiptStore
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
export const test  = /** @type {TestContextFn} */ (anyTest)
