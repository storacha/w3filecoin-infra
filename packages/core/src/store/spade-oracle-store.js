import { PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3'
import pRetry from 'p-retry'
import { StoreOperationFailed, RecordNotFound } from '@web3-storage/filecoin-api/errors'

import { connectBucket } from './index.js'


/**
 * Spade oracle store keeps the latest known contracts that spade put together.
 * 
 * @param {import('./types.js').BucketConnect | import('@aws-sdk/client-s3').S3Client} conf
 * @param {object} context
 * @param {string} context.name
 * @returns {import('./types.js').SpadeOracleStore}
 */
export function createClient (conf, context) {
  const bucketClient = connectBucket(conf)

  return {
    put: async (record) => {
      const putCmd = new PutObjectCommand({
        Bucket: context.name,
        Key: encodeURIComponent(record.key),
        Body: record.value
      })

      // retry to avoid throttling errors
      try {
        await pRetry(() => bucketClient.send(putCmd))
      } catch (/** @type {any} */ error) {
        console.log('err', error)
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
        Key: encodeURIComponent(key)
      })

      let res
      try {
        res = await bucketClient.send(putCmd)
      } catch (/** @type {any} */ error) {
        if (error?.$metadata.httpStatusCode === 404) {
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

      return {
        ok: {
          key,
          value: await res.Body.transformToByteArray()
        }
      }
    },
    has: async (key) => {
      const putCmd = new GetObjectCommand({
        Bucket: context.name,
        Key: encodeURIComponent(key)
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
          error: new RecordNotFound('item not found in store')
        }
      }

      return {
        ok: true
      }
    },
  }
}
