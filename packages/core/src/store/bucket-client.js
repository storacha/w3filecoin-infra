import {
  PutObjectCommand,
  GetObjectCommand,
  HeadObjectCommand
} from '@aws-sdk/client-s3'
import pRetry from 'p-retry'
import { RecordNotFound, StoreOperationFailed } from '@web3-storage/filecoin-api/errors'

import { connectBucket } from './index.js'

/**
 * @typedef {import('./types.js').BucketStoreRecord} BucketStoreRecord
 * @typedef {import('@aws-sdk/client-s3').GetObjectCommandOutput} GetObjectCommandOutput
 */

/**
 * @template Key
 * @template Record
 *
 * @param {import('./types.js').BucketConnect | import('@aws-sdk/client-s3').S3Client} conf
 * @param {object} context
 * @param {string} context.name
 * @param {(record: Record) => BucketStoreRecord} context.encodeRecord
 * @param {(key: Key) => string} context.encodeKey
 * @param {(encodedRecord: BucketStoreRecord) => Record} context.decodeRecord
 * @param {(res: GetObjectCommandOutput) => Promise<string | Uint8Array>} context.decodeBucketResponse
 * @returns {import('@web3-storage/filecoin-api/types').Store<Key, Record>}
 */
export function createBucketClient (conf, context) {
  const bucketClient = connectBucket(conf)

  return {
    put: async (record) => {
      const { key, body } = context.encodeRecord(record)
      const putCmd = new PutObjectCommand({
        Bucket: context.name,
        Key: key,
        Body: body
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
      const encodedKey = context.encodeKey(key)
      const getCmd = new GetObjectCommand({
        Bucket: context.name,
        Key: encodedKey,
      })

      let res
      try {
        res = await bucketClient.send(getCmd)
      } catch (/** @type {any} */ error) {
        if (error?.$metadata?.httpStatusCode === 404) {
          return {
            error: new RecordNotFound('item not found in store')
          }
        }
        return {
          error: new StoreOperationFailed(error.message)
        }
      }

      if (!res || !res.Body) {
        return {
          error: new RecordNotFound('item not found in store')
        }
      }

      const encodedRecord = await context.decodeBucketResponse(res)
      return {
        ok: context.decodeRecord({
          key: encodedKey,
          body: encodedRecord
        })
      }
    },
    has: async (key) => {
      const getCmd = new HeadObjectCommand({
        Bucket: context.name,
        Key: context.encodeKey(key),
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
    }
  }
}
