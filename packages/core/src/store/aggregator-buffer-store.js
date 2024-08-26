import { CBOR } from '@ucanto/core'
import { parseLink } from '@ucanto/server'
import { LRUCache } from 'lru-cache'

import { createBucketClient } from './bucket-client.js'

/**
 * @typedef {import('@web3-storage/filecoin-api/aggregator/api').BufferRecord} BufferRecord
 * @typedef {import('@web3-storage/filecoin-api/aggregator/api').Buffer} Buffer
 * @typedef {import('multiformats').ByteView<Buffer>} BufferStoreRecordBody
 * @typedef {{key: string, body: BufferStoreRecordBody}} BufferStoreRecord
 */

/**
 * @param {BufferRecord} record
 * @returns {BufferStoreRecord}
 */
const encodeRecord = (record) => {
  return {
    key: encodeKey(record.block),
    body: CBOR.encode(record.buffer)
  }
}

/**
 * @param {import('multiformats').Link} link
 * @returns {string}
 */
const encodeKey = (link) => {
  return `${link.toString()}/${link.toString()}`
}

/**
 * @param {import('@aws-sdk/client-s3').GetObjectCommandOutput} res 
 * @returns {Promise<Uint8Array>}
 */
const decodeBucketResponse = (res) => {
  // @ts-expect-error typescript do not get body will be there
  return res.Body.transformToByteArray()
}

/**
 * @param {BufferStoreRecord} encodedRecord
 * @returns {BufferRecord}
 */
const decodeRecord = (encodedRecord) => {
  /** @type {Buffer} */
  const buffer =  CBOR.decode(encodedRecord.body)

  return {
    block: parseLink(encodedRecord.key.split('/')[0]),
    buffer
  }
}

/**
 * @param {import('./types.js').BucketConnect | import('@aws-sdk/client-s3').S3Client} conf
 * @param {object} context
 * @param {string} context.name
 * @returns {import('@web3-storage/filecoin-api/aggregator/api').BufferStore}
 */
export function createClient (conf, context) {
  return createBucketClient(conf, {
    name: context.name,
    encodeRecord,
    encodeKey,
    decodeRecord,
    decodeBucketResponse
  })
}

const CACHE_MAX = 10_000

/**
 * @param {import('@web3-storage/filecoin-api/aggregator/api').BufferStore} bufferStore
 * @returns {import('@web3-storage/filecoin-api/aggregator/api').BufferStore}
 */
export const withCache = (bufferStore) => {
  /** @type {LRUCache<string, import('@web3-storage/filecoin-api/aggregator/api').BufferRecord>} */
  const cache = new LRUCache({ max: CACHE_MAX })
  return {
    ...bufferStore,
    async get (key) {
      const cacheKey = key.toString()
      const cached = cache.get(cacheKey)
      if (cached) return { ok: cached }
      const res = await bufferStore.get(key)
      if (res.ok) cache.set(cacheKey, res.ok)
      return res
    }
  }
}
