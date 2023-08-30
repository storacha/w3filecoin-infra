import { encode as JSONencode, decode as JSONdecode } from '@ipld/dag-json'
import { fromString } from 'uint8arrays/from-string'
import { toString } from 'uint8arrays/to-string'
import { parseLink } from '@ucanto/server'

/**
 * @typedef {import('@web3-storage/data-segment').PieceLink} PieceLink
 * @typedef {import('./types.js').Inclusion<PieceLink>} Data
 * @typedef {import('./types.js').Inclusion<string>} StoreRecord
 * @typedef {Pick<import('./types.js').Inclusion<string>, "aggregate" | "piece">} StoreKey
 */

/**
 * @type {import('./types').Encoder<Data, Data, StoreRecord, StoreKey>}
 */
export const encode = {
  /**
   * Encode data structure to store record.
   */
  storeRecord: async (inclusion) => {
    return Promise.resolve({
      ...inclusion,
      aggregate: inclusion.aggregate.toString(),
      piece: inclusion.piece.toString(),
      insertedAt: inclusion.insertedAt || Date.now()
    })
  },
  /**
   * Encode key from store record.
   */
  storeKey: async (inclusion) => {
    return Promise.resolve({
      aggregate: inclusion.aggregate.toString(),
      piece: inclusion.piece.toString()
    })
  },
  /**
   * Encode data structure to queue message.
   */
  message: async (aggregate) => {
    const encodedBytes = JSONencode(aggregate)
    return Promise.resolve(toString(encodedBytes))
  }
}

/**
 * @type {import('./types').Decoder<Data, StoreRecord, Data>}
 */
export const decode = {
  /**
   * Decode data structure from stored record.
   */
  storeRecord: (storeRecord) => {
    return Promise.resolve({
      aggregate: parseLink(storeRecord.aggregate),
      piece: parseLink(storeRecord.piece),
      insertedAt: storeRecord.insertedAt,
      submitedAt: storeRecord.submitedAt,
      resolvedAt: storeRecord.resolvedAt,
      failedReason: storeRecord.failedReason,
      stat: storeRecord.stat
    })
  },
  /**
   * Decode data structure from queue message.
   */
  message: (messageBody) => {
    const decodedBytes = fromString(messageBody)
    return Promise.resolve(JSONdecode(decodedBytes))
  }
}