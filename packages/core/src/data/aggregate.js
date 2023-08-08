import { encode as JSONencode, decode as JSONdecode } from '@ipld/dag-json'
import { fromString } from 'uint8arrays/from-string'
import { toString } from 'uint8arrays/to-string'
import { parseLink } from '@ucanto/server'

/**
 * @typedef {import('@web3-storage/data-segment').PieceLink} PieceLink
 * @typedef {import('@web3-storage/data-segment').LegacyPieceLink} LegacyPieceLink
 * @typedef {import('@ucanto/interface').UnknownLink} UnknownLink
 * @typedef {import('./types.js').Aggregate<LegacyPieceLink, UnknownLink>} Data
 * @typedef {import('./types.js').Aggregate<string, string>} StoreRecord
 * @typedef {Pick<import('./types.js').Aggregate<string, string>, "piece">} StoreKey
 */

/**
 * Encoding of type aggregate to store and messages.
 *
 * @type {import('./types').Encoder<Data, StoreRecord, StoreKey>}
 */
export const encode = {
  /**
   * Encode data structure to store record.
   */
  storeRecord: async (aggregate) => {
    return Promise.resolve({
      ...aggregate,
      piece: aggregate.piece.toString(),
      buffer: aggregate.buffer.toString(),
      invocation: aggregate.invocation?.toString(),
      task: aggregate.task?.toString(),
      insertedAt: aggregate.insertedAt || Date.now()
    })
  },
  /**
   * Encode key from store record.
   */
  storeKey: async (aggregate) => {
    return Promise.resolve({
      piece: aggregate.piece.toString()
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
 * Decoding of type aggregate to store and messages.
 *
 * @type {import('./types').Decoder<Data, StoreRecord, Data>}
 */
export const decode = {
  /**
   * Decode stored record into data structure.
   */
  storeRecord: (storeRecord) => {
    return Promise.resolve({
      piece: parseLink(storeRecord.piece),
      buffer: parseLink(storeRecord.buffer),
      invocation: storeRecord.invocation ? parseLink(storeRecord.invocation) : undefined,
      task: storeRecord.task ? parseLink(storeRecord.task) : undefined,
      insertedAt: storeRecord.insertedAt,
      storefront: storeRecord.storefront,
      group: storeRecord.group,
      stat: storeRecord.stat
    })
  },
  /**
   * Decode queue message into data structure.
   */
  message: (messageBody) => {
    const decodedBytes = fromString(messageBody)
    return Promise.resolve(JSONdecode(decodedBytes))
  }
}
