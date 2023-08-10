import { encode as JSONencode, decode as JSONdecode } from '@ipld/dag-json'
import { CBOR } from '@ucanto/core'
import { fromString } from 'uint8arrays/from-string'
import { toString } from 'uint8arrays/to-string'

/**
 * @typedef {import('@web3-storage/data-segment').PieceLink} PieceLink
 * @typedef {import('./types.js').Buffer<PieceLink>} Data
 * @typedef {{ key: string, value: Uint8Array }} StoreRecord
 * @typedef {import('multiformats').Link<import('./types.js').Buffer<PieceLink>>} MessageCid
 * @typedef {{ cid: MessageCid }} MessageRecord
 * @typedef {string} StoreKey
 */

/**
 * @type {import('./types').Encoder<Data, StoreRecord, StoreKey>}
 */
export const encode = {
  /**
   * Encode data structure to store record.
   */
  storeRecord: async (buffer) => {
    const block = await encodeBlock(buffer)
    return {
      key: `${block.cid}/${block.cid}`,
      value: block.bytes
    }
  },
  /**
   * Encode key from store record.
   */
  storeKey: async (buffer) => {
    const block = await encodeBlock(buffer)
    return `${block.cid}/${block.cid}`
  },
  /**
   * Encode data structure to queue message.
   */
  message: async (buffer) => {
    const block = await encodeBlock(buffer)
    /** @type {MessageRecord} */
    const messageRecord = {
      cid: block.cid
    }
    const encodedBytes = JSONencode(messageRecord)
    return toString(encodedBytes)
  }
}

/**
 * @type {import('./types').Decoder<Data, StoreRecord, MessageRecord>}
 */
export const decode = {
  /**
   * Decode data structure from queue message.
   */
  storeRecord: async (storeRecord) => {
    return await decodeBlock(storeRecord.value)
  },
  /**
   * Decode data structure from queue message.
   */
  message: (messageBody) => {
    const decodedBytes = fromString(messageBody)
    return Promise.resolve(JSONdecode(decodedBytes))
  }
}

/**
 * @param {import('./types.js').Buffer<PieceLink>} buffer
 */
export async function encodeBlock (buffer) {
  const block = await CBOR.write(buffer)

  return block
}

/**
 * @param {Uint8Array} bytes
 */
async function decodeBlock (bytes) {
  /** @type {import('./types.js').Buffer<PieceLink>} */
  const block = await CBOR.decode(bytes)

  return block
}
