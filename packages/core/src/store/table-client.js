import { PutItemCommand, GetItemCommand } from '@aws-sdk/client-dynamodb'
import { marshall } from '@aws-sdk/util-dynamodb'
import pRetry from 'p-retry'

import { connectTable } from './index.js'

/**
 * @template R
 * @template K
 *
 * @param {import('./types.js').TableConnect | import('@aws-sdk/client-dynamodb').DynamoDBClient} conf
 * @param {object} context
 * @param {string} context.tableName
 * @param {(item: R) => Record<string, any>} context.encodeRecord
 * @param {(item: Record<string, any>) => R} context.decodeRecord
 * @param {(key: K) => Record<string, any>} context.encodeKey
 * @returns {import('./types.js').ExtendedStore<R, K>}
 */
export function createTableStoreClient (conf, context) {
  const tableclient = connectTable(conf)

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

      const putCmd = new PutItemCommand({
        TableName: context.tableName,
        Item: marshall(encodedRecord),
      })

      // retry to avoid throttling errors
      try {
        await pRetry(() => tableclient.send(putCmd))
      } catch (/** @type {any} */ err) {
        return {
          // TODO: specify error
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

      const getCmd = new GetItemCommand({
        TableName: context.tableName,
        Key: encodedKey
      })

      let res
      try {
        res = await tableclient.send(getCmd)
      } catch (/** @type {any} */ err) {
        return {
          // TODO: specify error
          error: err
        }
      }

      return {
        ok: context.decodeRecord(res)
      }
    }
  }
}
