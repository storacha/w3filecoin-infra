import { PutItemCommand, GetItemCommand } from '@aws-sdk/client-dynamodb'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'

import { StoreOperationFailed, StoreNotFound, EncodeRecordFailed } from '@web3-storage/filecoin-api/errors'

import { connectTable } from './index.js'

/**
 * @template Data
 * @template StoreRecord
 * @template StoreKey
 *
 * @param {import('./types.js').TableConnect | import('@aws-sdk/client-dynamodb').DynamoDBClient} conf
 * @param {object} context
 * @param {string} context.tableName
 * @param {(data: Data) => Promise<StoreRecord>} context.encodeRecord
 * @param {(item: StoreRecord) => Promise<Data>} context.decodeRecord
 * @param {(data: Data) => Promise<StoreKey>} context.encodeKey
 * @returns {import('@web3-storage/filecoin-api/types').Store<Data>}
 */
export function createTableStoreClient (conf, context) {
  const tableclient = connectTable(conf)

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

      const putCmd = new PutItemCommand({
        TableName: context.tableName,
        Item: marshall(encodedRecord, {
          removeUndefinedValues: true
        }),
      })

      // retry to avoid throttling errors
      try {
        await tableclient.send(putCmd)
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
      let encodedKey
      try {
        encodedKey = await context.encodeKey(key)
      } catch (/** @type {any} */ error) {
        return {
          error: new EncodeRecordFailed(error.message)
        }
      }

      const getCmd = new GetItemCommand({
        TableName: context.tableName,
        Key: marshall(encodedKey)
      })

      let res
      try {
        res = await tableclient.send(getCmd)
      } catch (/** @type {any} */ error) {
        return {
          error: new StoreOperationFailed(error.message)
        }
      }

      // not found error
      if (!res.Item) {
        return {
          error: new StoreNotFound('item not found in store')
        }
      }

      return {
        ok: await context.decodeRecord(
          /** @type {StoreRecord} */ (unmarshall(res.Item))
        )
      }
    }
  }
}
