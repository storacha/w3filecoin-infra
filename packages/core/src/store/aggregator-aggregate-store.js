import { PutItemCommand, GetItemCommand } from '@aws-sdk/client-dynamodb'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'
import { RecordNotFound, StoreOperationFailed } from '@web3-storage/filecoin-api/errors'
import { parseLink } from '@ucanto/server'

import { connectTable } from './index.js'

/**
 * @typedef {import('@web3-storage/filecoin-api/aggregator/api').AggregateRecord} AggregateRecord
 * @typedef {import('@web3-storage/filecoin-api/aggregator/api').AggregateRecordKey} AggregateRecordKey
 * @typedef {import('./types').InferStoreRecord<AggregateRecord>} InferStoreRecord
 * @typedef {Pick<InferStoreRecord, 'aggregate'>} AggregateStoreRecordKey
 */

/**
 * @param {AggregateRecord} record 
 * @returns {InferStoreRecord} 
 */
const encodeRecord = (record) => {
  return {
    ...record,
    aggregate: record.aggregate.toString(),
    pieces: record.pieces.toString(),
  }
}

/**
 * @param {AggregateRecordKey} recordKey 
 * @returns {AggregateStoreRecordKey} 
 */
const encodeKey = (recordKey) => {
  return {
    aggregate: recordKey.aggregate.toString(),
  }
}

/**
 * @param {InferStoreRecord} encodedRecord 
 * @returns {AggregateRecord}
 */
export const decodeRecord = (encodedRecord) => {
  return {
    ...encodedRecord,
    aggregate: parseLink(encodedRecord.aggregate),
    pieces: parseLink(encodedRecord.pieces)
  }
}

/**
 * @param {import('./types.js').TableConnect | import('@aws-sdk/client-dynamodb').DynamoDBClient} conf
 * @param {object} context
 * @param {string} context.tableName
 * @returns {import('@web3-storage/filecoin-api/aggregator/api').AggregateStore}
 */
export function createClient (conf, context) {
  const tableclient = connectTable(conf)

  return {
    put: async (record) => {
      const putCmd = new PutItemCommand({
        TableName: context.tableName,
        Item: marshall(encodeRecord(record), {
          removeUndefinedValues: true
        }),
      })

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
      const getCmd = new GetItemCommand({
        TableName: context.tableName,
        Key: marshall(encodeKey(key)),
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
          error: new RecordNotFound('item not found in store')
        }
      }

      return {
        ok: decodeRecord(
          /** @type {InferStoreRecord} */ (unmarshall(res.Item))
        )
      }
    },
    has: async (key) => {
      const getCmd = new GetItemCommand({
        TableName: context.tableName,
        Key: marshall(encodeKey(key)),
      })
      let res
      try {
        res = await tableclient.send(getCmd)
      } catch (/** @type {any} */ error) {
        return {
          error: new StoreOperationFailed(error.message)
        }
      }

      // not found
      if (!res.Item) {
        return {
          ok: false
        }
      }

      return {
        ok: true
      }
    }
  }
}
