import {
  PutItemCommand,
  GetItemCommand,
  QueryCommand
} from '@aws-sdk/client-dynamodb'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'
import {
  RecordNotFound,
  StoreOperationFailed
} from '@storacha/filecoin-api/errors'
import { parseLink } from '@ucanto/server'

import { connectTable } from './index.js'

/**
 * @typedef {import('@storacha/filecoin-api/deal-tracker/api').DealRecord} DealRecord
 * @typedef {import('@storacha/filecoin-api/deal-tracker/api').DealRecordKey} DealRecordKey
 * @typedef {import('@storacha/filecoin-api/deal-tracker/api').DealRecordQueryByPiece} DealRecordQueryByPiece
 * @typedef {import('./types.js').InferStoreRecord<DealRecord>} InferStoreRecord
 * @typedef {Pick<InferStoreRecord, 'piece' | 'dealId'>} DealStoreRecordKey
 * @typedef {Pick<InferStoreRecord, 'piece'>} DealStoreRecordQueryByPiece
 */

/**
 * @param {DealRecord} record
 * @returns {InferStoreRecord}
 */
const encodeRecord = (record) => {
  return {
    ...record,
    piece: record.piece.toString()
  }
}

/**
 * @param {DealRecordKey} recordKey
 * @returns {DealStoreRecordKey}
 */
const encodeKey = (recordKey) => {
  return {
    ...recordKey,
    piece: recordKey.piece.toString()
  }
}

/**
 * @param {DealRecordQueryByPiece} recordKey
 * @returns {DealStoreRecordQueryByPiece}
 */
const encodeQueryByPiece = (recordKey) => {
  return {
    ...recordKey,
    piece: recordKey.piece.toString()
  }
}

/**
 * @param {InferStoreRecord} encodedRecord
 * @returns {DealRecord}
 */
const decodeRecord = (encodedRecord) => {
  return {
    ...encodedRecord,
    piece: parseLink(encodedRecord.piece)
  }
}

/**
 * @param {import('./types.js').TableConnect | import('@aws-sdk/client-dynamodb').DynamoDBClient} conf
 * @param {object} context
 * @param {string} context.tableName
 * @returns {import('@storacha/filecoin-api/deal-tracker/api').DealStore}
 */
export function createClient (conf, context) {
  const tableclient = connectTable(conf)

  return {
    put: async (record) => {
      const putCmd = new PutItemCommand({
        TableName: context.tableName,
        Item: marshall(encodeRecord(record), {
          removeUndefinedValues: true
        })
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
        Key: marshall(encodeKey(key))
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
        Key: marshall(encodeKey(key))
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
          ok: false
        }
      }

      return {
        ok: true
      }
    },
    query: async (search, options) => {
      const dealStoreRecordQueryByPiece = encodeQueryByPiece(search)
      const queryCmd = new QueryCommand({
        TableName: context.tableName,
        IndexName: 'piece',
        KeyConditions: {
          piece: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ S: dealStoreRecordQueryByPiece.piece }]
          }
        },
        ExclusiveStartKey: options?.cursor
          ? JSON.parse(options.cursor)
          : undefined,
        Limit: options?.size
      })

      let res
      try {
        res = await tableclient.send(queryCmd)
      } catch (/** @type {any} */ error) {
        console.error(error)
        return {
          error: new StoreOperationFailed(error.message)
        }
      }

      return {
        ok: {
          results: (res.Items ?? []).map((item) =>
            decodeRecord(/** @type {InferStoreRecord} */ (unmarshall(item)))
          ),
          ...(res.LastEvaluatedKey
            ? { cursor: JSON.stringify(res.LastEvaluatedKey) }
            : {})
        }
      }
    }
  }
}
