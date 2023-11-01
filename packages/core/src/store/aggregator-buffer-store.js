import { CBOR } from '@ucanto/core'
import { parseLink } from '@ucanto/server'

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
    key: record.block.toString(),
    body: CBOR.encode(record.buffer)
  }
}

/**
 * @param {import('multiformats').Link} link
 * @returns {string}
 */
const encodeKey = (link) => {
  return link.toString()
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
    block: parseLink(encodedRecord.key),
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
