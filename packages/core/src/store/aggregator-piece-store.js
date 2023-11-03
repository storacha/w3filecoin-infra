import { PutItemCommand, GetItemCommand, UpdateItemCommand } from '@aws-sdk/client-dynamodb'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'
import { RecordNotFound, StoreOperationFailed } from '@web3-storage/filecoin-api/errors'
import { parseLink } from '@ucanto/server'

import { connectTable } from './index.js'

/**
 * @typedef {'offered' | 'accepted'} PieceStatus
 * @typedef {import('@web3-storage/filecoin-api/aggregator/api').PieceRecord} PieceRecord
 * @typedef {import('@web3-storage/filecoin-api/aggregator/api').PieceRecordKey} PieceRecordKey
 * @typedef {import('./types').InferStoreRecord<PieceRecord>} InferStoreRecord
 * @typedef {import('./types').AggregatorPieceStoreRecord} PieceStoreRecord
 * @typedef {import('./types').AggregatorPieceStoreRecordStatus} PieceStoreRecordStatus
 * @typedef {Pick<InferStoreRecord, 'piece' | 'group'>} DealStoreRecordKey
 */

/**
 * @param {PieceRecord} record 
 * @returns {PieceStoreRecord} 
 */
const encodeRecord = (record) => {
  return {
    piece: record.piece.toString(),
    stat: encodeStatus(record.status),
    group: record.group,
    insertedAt: record.insertedAt,
    updatedAt: record.updatedAt,
  }
}

/**
 * @param {Partial<PieceRecord>} record 
 * @returns {Partial<PieceStoreRecord>} 
 */
const encodePartialRecord = (record) => {
  return {
    ...(record.status && { stat: encodeStatus(record.status) }),
    ...(record.updatedAt && { updatedAt: record.updatedAt }),
  }
}

/**
 * 
 * @param {PieceStatus} status 
 * @returns 
 */
const encodeStatus = (status) => {
  switch (status) {
    case 'offered': {
      return Status.OFFERED
    }
    case 'accepted': {
      return Status.ACCEPTED
    }
    default: {
      throw new Error('invalid status received for encoding')
    }
  }
}

/**
 * @param {PieceRecordKey} recordKey 
 * @returns {DealStoreRecordKey} 
 */
const encodeKey = (recordKey) => {
  return {
    ...recordKey,
    piece: recordKey.piece.toString()
  }
}

/**
 * @param {PieceStoreRecord} encodedRecord 
 * @returns {PieceRecord}
 */
export const decodeRecord = (encodedRecord) => {
  return {
    ...encodedRecord,
    piece: parseLink(encodedRecord.piece),
    status: decodeStatus(encodedRecord.stat)
  }
}

/**
 * @param {PieceStoreRecordStatus} status 
 * @returns {PieceStatus}
 */
const decodeStatus = (status) => {
  switch (status) {
    case Status.OFFERED: {
      return 'offered'
    }
    case Status.ACCEPTED: {
      return 'accepted'
    }
    default: {
      throw new Error('invalid status received for decoding')
    }
  }
}

/**
 * @param {import('./types.js').TableConnect | import('@aws-sdk/client-dynamodb').DynamoDBClient} conf
 * @param {object} context
 * @param {string} context.tableName
 * @returns {import('@web3-storage/filecoin-api/aggregator/api').PieceStore}
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
          /** @type {PieceStoreRecord} */ (unmarshall(res.Item))
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
    },
    update: async (key, record) => {
      const encodedRecord = encodePartialRecord(record)
      const ExpressionAttributeValues = {
        ':ua': { S: encodedRecord.updatedAt || (new Date()).toISOString() },
        ...(encodedRecord.stat && {':st': { N: `${encodedRecord.stat}` }})
      }

      const stateUpdateExpression = encodedRecord.stat ? ', stat = :st' : ''
      const UpdateExpression = `SET updatedAt = :ua ${stateUpdateExpression}`

      const updateCmd = new UpdateItemCommand({
        TableName: context.tableName,
        Key: marshall(encodeKey(key)),
        UpdateExpression,
        ExpressionAttributeValues,
        ReturnValues: 'ALL_NEW',
      })

      let res
      try {
        res = await tableclient.send(updateCmd)
      } catch (/** @type {any} */ error) {
        return {
          error: new StoreOperationFailed(error.message)
        }
      }

      if (!res.Attributes) {
        return {
          error: new StoreOperationFailed('Missing `Attributes` property on DyanmoDB response')
        }
      }

      return {
        ok: decodeRecord(
          /** @type {PieceStoreRecord} */ (unmarshall(res.Attributes))
        )
      }
    }
  }
}

export const Status = {
  OFFERED: 0,
  ACCEPTED: 1,
}
