import {
  PutObjectCommand,
  GetObjectCommand
} from '@aws-sdk/client-s3'
import pRetry from 'p-retry'
import { StoreOperationFailed, StoreNotFound, EncodeRecordFailed } from '@web3-storage/filecoin-api/errors'

import { connectBucket } from './index.js'

/**
 * @typedef {{ key: string, value: Uint8Array}} StoreRecord
 * @typedef {string} StoreKey
 */

/**
 * @template Data
 *
 * @param {import('./types.js').BucketConnect | import('@aws-sdk/client-s3').S3Client} conf
 * @param {object} context
 * @param {string} context.name
 * @param {(data: Data) => Promise<StoreRecord>} context.encodeRecord
 * @param {(item: StoreRecord) => Promise<Data>} context.decodeRecord
 * @returns {import('@web3-storage/filecoin-api/types').Store<Data>}
 */
export function createBucketStoreClient (conf, context) {
  const bucketClient = connectBucket(conf)

  return {
    put: async (record) => {
      let encodedRecord
      try {
        encodedRecord = await context.encodeRecord(record)
      } catch (/** @type {any} */ error) {
        return {
          error: new EncodeRecordFailed(error.message)
        }
      }

      const putCmd = new PutObjectCommand({
        Bucket: context.name,
        Key: encodedRecord.key,
        Body: encodedRecord.value
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
      const putCmd = new GetObjectCommand({
        Bucket: context.name,
        Key: key
      })

      let res
      try {
        res = await bucketClient.send(putCmd)
      } catch (/** @type {any} */ error) {
        return {
          error: new StoreOperationFailed(error.message)
        }
      }

      if (!res || !res.Body) {
        return {
          error: new StoreNotFound('item not found in store')
        }
      }

      return {
        ok: await context.decodeRecord({
          key,
          value: await res.Body.transformToByteArray()
        })
      }
    }
  }
}
