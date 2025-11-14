import { CBOR } from '@ucanto/core'
import { parseLink } from '@ucanto/server'

import { createBucketClient } from './bucket-client.js'

/**
 * @typedef {import('@storacha/capabilities/types').InclusionProof} InclusionProof
 * @typedef {import('./types.js').InclusionProofRecord} InclusionProofRecord
 * @typedef {import('multiformats').ByteView<InclusionProof>} InclusionProofStoreRecordBody
 * @typedef {{key: string, body: InclusionProofStoreRecordBody}} InclusionProofStoreRecord
 */

/**
 * @param {InclusionProofRecord} record
 * @returns {InclusionProofStoreRecord}
 */
const encodeRecord = (record) => {
  return {
    key: record.block.toString(),
    body: CBOR.encode(record.inclusion)
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
 * @param {InclusionProofStoreRecord} encodedRecord
 * @returns {InclusionProofRecord}
 */
const decodeRecord = (encodedRecord) => {
  /** @type {InclusionProof} */
  const inclusion = CBOR.decode(encodedRecord.body)

  return {
    block: parseLink(encodedRecord.key),
    inclusion
  }
}

/**
 * @param {import('./types.js').BucketConnect | import('@aws-sdk/client-s3').S3Client} conf
 * @param {object} context
 * @param {string} context.name
 * @returns {import('./types.js').InclusionProofStore}
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
