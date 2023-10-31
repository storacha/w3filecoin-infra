import {
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand
} from '@aws-sdk/client-s3'
import pRetry from 'p-retry'
import { RecordNotFound, StoreOperationFailed } from '@web3-storage/filecoin-api/errors'
import { parseLink } from '@ucanto/server'

import { createBucketClient } from './bucket-client.js'
import { connectBucket } from './index.js'

/**
 * @typedef {import('@web3-storage/filecoin-api/dealer/api').OfferDocument} OfferDocument
 * @typedef {import('./types.js').DealerOfferStoreRecordValue} DealerOfferStoreRecordValue
 * @typedef {string} OfferDocumentStoreRecordBody
 * @typedef {{key: string, body: OfferDocumentStoreRecordBody}} OfferDocumentStoreRecord
 */

/**
 * @param {OfferDocument} record
 * @returns {OfferDocumentStoreRecord}
 */
const encodeRecord = (record) => {
  const body = JSON.stringify(/** @type {DealerOfferStoreRecordValue} */ ({
    aggregate: record.value.aggregate.toString(),
    pieces: record.value.pieces.map(p => p.toString()),
    collection: record.value.issuer,
    orderID: Date.now()
  }))

  return {
    body,
    key: record.key
  }
}

/**
 * @param {string} key
 * @returns {string}
 */
const encodeKey = (key) => {
  return key
}

/**
 * @param {import('@aws-sdk/client-s3').GetObjectCommandOutput} res 
 * @returns {Promise<string>}
 */
const decodeBucketResponse = (res) => {
  // @ts-expect-error typescript do not get body will be there
  return res.Body.transformToString()
}

/**
 * @param {OfferDocumentStoreRecord} encodedRecord
 * @returns {OfferDocument}
 */
const decodeRecord = (encodedRecord) => {
  /** @type {DealerOfferStoreRecordValue} */
  const record =  JSON.parse(encodedRecord.body)

  return {
    key: encodedRecord.key,
    value: {
      aggregate: parseLink(record.aggregate),
      pieces: record.pieces.map(p => parseLink(p)),
      issuer: /** @type {`did:${string}:${string}`} */ (record.collection),
    }
  }
}

/**
 * @param {import('./types.js').BucketConnect | import('@aws-sdk/client-s3').S3Client} conf
 * @param {object} context
 * @param {string} context.name
 * @returns {import('@web3-storage/filecoin-api/dealer/api').OfferStore<OfferDocument>}
 */
export function createClient (conf, context) {
  const bucketClient = connectBucket(conf)

  return {
    ...createBucketClient(conf, {
      name: context.name,
      encodeRecord,
      encodeKey,
      decodeRecord,
      decodeBucketResponse
    }),
    update: async (key, record) => {
      if (!record.key) {
        return {
          error: new StoreOperationFailed('new key was not provided for update')
        }
      }

      const getCmd = new GetObjectCommand({
        Bucket: context.name,
        Key: encodeKey(key)
      })

      let res
      try {
        res = await bucketClient.send(getCmd)
      } catch (/** @type {any} */ error) {
        return {
          error: new StoreOperationFailed(error.message)
        }
      }

      if (!res || !res.Body) {
        return {
          error: new RecordNotFound('item not found in store')
        }
      }

      const encodedRecord = await decodeBucketResponse(res)
      const putCmd = new PutObjectCommand({
        Bucket: context.name,
        Key: encodeKey(record.key),
        Body: encodedRecord
      })

      // retry to avoid throttling errors
      try {
        await pRetry(() => bucketClient.send(putCmd))
      } catch (/** @type {any} */ error) {
        return {
          error: new StoreOperationFailed(error.message)
        }
      }

      // Should delete old record when copied
      const deleteCmd = new DeleteObjectCommand({
        Bucket: context.name,
        Key: key
      })

      try {
        await pRetry(() => bucketClient.send(deleteCmd))
      } catch (/** @type {any} */ error) {
        return {
          error: new StoreOperationFailed(error.message)
        }
      }

      return {
        ok: decodeRecord({
          key: encodeKey(record.key),
          body: encodedRecord
        })
      }
    }
  }
}
