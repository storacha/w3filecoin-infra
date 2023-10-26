import {
  PutObjectCommand,
  GetObjectCommand
} from '@aws-sdk/client-s3'
import pRetry from 'p-retry'
import { RecordNotFound, StoreOperationFailed } from '@web3-storage/filecoin-api/errors'
import { parseLink } from '@ucanto/server'

import { connectBucket } from './index.js'

/**
 * @typedef {import('@web3-storage/filecoin-api/dealer/api').OfferDocument} OfferDocument
 * @typedef {import('@web3-storage/filecoin-api/dealer/api').OfferDocument} OfferValue
 * @typedef {import('./types.js').DealerOfferStoreRecordValue} DealerOfferStoreRecordValue
 */

/**
 * @param {OfferDocument} record 
 */
const encodeRecord = (record) => {
  return JSON.stringify(/** @type {DealerOfferStoreRecordValue} */ ({
    aggregate: record.value.aggregate.toString(),
    pieces: record.value.pieces.map(p => p.toString()),
    collection: record.value.issuer,
    orderID: Date.now()
  }))
}

/**
 * @param {string} key
 * @param {string} encodedRecord
 * @returns {OfferDocument}
 */
const decodeRecord = (key, encodedRecord) => {
  /** @type {DealerOfferStoreRecordValue} */
  const record =  JSON.parse(encodedRecord)

  return {
    key: key,
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
    put: async (record) => {
      const putCmd = new PutObjectCommand({
        Bucket: context.name,
        Key: record.key,
        Body: encodeRecord(record)
      })

      // retry to avoid throttling errors
      try {
        await pRetry(() => bucketClient.send(putCmd))
      } catch (/** @type {any} */ error) {
        return {
          error: new StoreOperationFailed(error.message)
        }
      }

      return {
        ok: {}
      }
    },
    get: async (key) => {
      const getCmd = new GetObjectCommand({
        Bucket: context.name,
        Key: key
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

      const encodedRecord = await res.Body.transformToString()
      return {
        ok: decodeRecord(key, encodedRecord)
      }
    },
    has: async (key) => {
      const getCmd = new GetObjectCommand({
        Bucket: context.name,
        Key: key
      })

      try {
        await bucketClient.send(getCmd)
      } catch (/** @type {any} */ error) {
        if (error?.$metadata?.httpStatusCode === 404) {
          return {
            ok: false
          }
        }
        return {
          error: new StoreOperationFailed(error.message)
        }
      }

      return {
        ok: true
      }
    },
    update: async (key, record) => {
      if (!record.key) {
        return {
          error: new StoreOperationFailed('new key was not provided for update')
        }
      }

      const getCmd = new GetObjectCommand({
        Bucket: context.name,
        Key: key
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

      const encodedRecord = await res.Body.transformToString()
      const putCmd = new PutObjectCommand({
        Bucket: context.name,
        Key: record.key,
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

      return {
        ok: decodeRecord(record.key, encodedRecord)
      }
    }
  }
}
