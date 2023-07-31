import {
  PutObjectCommand,
  GetObjectCommand
} from '@aws-sdk/client-s3'
import pRetry from 'p-retry'

import { connectBucket } from './index.js'

/**
 * @template R
 * @template K
 *
 * @param {import('./types.js').BucketConnect | import('@aws-sdk/client-s3').S3Client} conf
 * @param {object} context
 * @param {string} context.name
 * @param {(item: R) => { key: string, value: Uint8Array}} context.encodeRecord
 * @param {(item: Record<string, any>) => R} context.decodeRecord
 * @param {(key: K) => string} context.encodeKey
 * @returns {import('./types.js').ExtendedStore<R, K>}
 */
export function createBucketStoreClient (conf, context) {
  const bucketClient = connectBucket(conf)

  return {
    put: async (record) => {
      let encodedRecord
      try {
        encodedRecord = context.encodeRecord(record)
      } catch (/** @type {any} */ err) {
        return {
          // TODO: specify error
          error: err
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
      } catch (/** @type {any} */ err) {
        return {
          error: err
        }
      }

      return {
        ok: {}
      }
    },
    get: async (key) => {
      let encodedKey
      try {
        encodedKey = context.encodeKey(key)
      } catch (/** @type {any} */ err) {
        return {
          // TODO: specify error
          error: err
        }
      }

      const putCmd = new GetObjectCommand({
        Bucket: context.name,
        Key: encodedKey
      })

      let res
      // retry to avoid throttling errors
      try {
        res = await pRetry(() => bucketClient.send(putCmd))
      } catch (/** @type {any} */ err) {
        return {
          error: err
        }
      }

      if (!res || !res.Body) {
        return {
          error: new Error('not found')
        }
      }

      return {
        ok: context.decodeRecord(await res.Body.transformToByteArray())
      }
    }
  }
}
