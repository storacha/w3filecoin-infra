import { encode as JSONencode, decode as JSONdecode } from '@ipld/dag-json'
import { fromString } from 'uint8arrays/from-string'
import { toString } from 'uint8arrays/to-string'
import { parseLink } from '@ucanto/server'

/**
 * @typedef {import('@web3-storage/data-segment').PieceLink} PieceLink
 */

/**
 * @type {import('./types').Encoder<import('./types.js').Piece<PieceLink>>}
 */
export const encode = {
  /**
   * Encode piece data structure to store record.
   */
  storeRecord: (piece) => {
    return {
      ...piece,
      piece: piece.piece.toString(),
      insertedAt: piece.insertedAt || Date.now()
    }
  },
  /**
   * Encode key from store record.
   *
   * @param {Omit<import('./types.js').Piece<PieceLink>, "insertedAt" | "group">} piece
   * @returns {Omit<import('./types.js').Piece<string>, "insertedAt" | "group">}
   */
  storeKey: (piece) => {
    return {
      piece: piece.piece.toString(),
      space: piece.space
    }
  },
  /**
   * Encode piece data structure to queue message.
   */
  message: (piece) => {
    const encodedBytes = JSONencode(piece)
    return toString(encodedBytes)
  }
}

/**
 * @type {import('./types').Decoder<import('./types.js').Piece<PieceLink>>}
 */
export const decode = {
  /**
   * Decode piece data structure from queue message.
   */
  storeRecord: (pieceRecord) => {
    return {
      piece: parseLink(pieceRecord.piece),
      space: pieceRecord.space,
      group: pieceRecord.group,
      insertedAt: pieceRecord.insertedAt,
    }
  },
  /**
   * Decode piece data structure from queue message.
   */
  message: (pieceMakerItem) => {
    const decodedBytes = fromString(pieceMakerItem)
    return JSONdecode(decodedBytes)
  }
}
