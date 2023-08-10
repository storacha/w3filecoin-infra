import { encode as JSONencode, decode as JSONdecode } from '@ipld/dag-json'
import { fromString } from 'uint8arrays/from-string'
import { toString } from 'uint8arrays/to-string'
import { parseLink } from '@ucanto/server'

/**
 * @typedef {import('@web3-storage/data-segment').PieceLink} PieceLink
 * @typedef {import('./types.js').Piece<PieceLink>} Data
 * @typedef {import('./types.js').Piece<string>} StoreRecord
 * @typedef {Omit<import('./types.js').Piece<string>, "insertedAt" | "group">} StoreKey
 */

/**
 * @type {import('./types').Encoder<Data, StoreRecord, StoreKey>}
 */
export const encode = {
  /**
   * Encode piece data structure to store record.
   */
  storeRecord: (piece) => {
    return Promise.resolve({
      ...piece,
      piece: piece.piece.toString(),
      insertedAt: piece.insertedAt || Date.now()
    })
  },
  /**
   * Encode key from store record.
   *
   * @param {import('./types.js').Piece<PieceLink>} piece
   * @returns {Promise<StoreKey>}
   */
  storeKey: (piece) => {
    return Promise.resolve({
      piece: piece.piece.toString(),
      storefront: piece.storefront
    })
  },
  /**
   * Encode piece data structure to queue message.
   */
  message: (piece) => {
    const encodedBytes = JSONencode(piece)
    return Promise.resolve(toString(encodedBytes))
  }
}

/**
 * @type {import('./types').Decoder<Data, StoreRecord, Data>}
 */
export const decode = {
  /**
   * Decode piece data structure from queue message.
   */
  storeRecord: (storeRecord) => {
    return Promise.resolve({
      piece: parseLink(storeRecord.piece),
      storefront: storeRecord.storefront,
      group: storeRecord.group,
      insertedAt: storeRecord.insertedAt,
    })
  },
  /**
   * Decode piece data structure from queue message.
   */
  message: (messageBody) => {
    const decodedBytes = fromString(messageBody)
    return Promise.resolve(JSONdecode(decodedBytes))
  }
}
