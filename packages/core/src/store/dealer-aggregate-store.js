import {
  PutItemCommand,
  GetItemCommand,
  UpdateItemCommand,
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
 * @typedef {import('@web3-storage/data-segment').PieceLink} PieceLink
 * @typedef {'offered' | 'accepted' | 'invalid'} AggregateStatus
 * @typedef {import('@storacha/filecoin-api/dealer/api').AggregateRecord} AggregateRecord
 * @typedef {import('@storacha/filecoin-api/dealer/api').AggregateRecordKey} AggregateRecordKey
 * @typedef {{ status: AggregateStatus }} AggregateRecordQuery
 * @typedef {import('./types.js').DealerAggregateStoreRecord} DealerAggregateStoreRecord
 * @typedef {import('./types.js').DealerAggregateStoreRecordKey} DealerAggregateStoreRecordKey
 * @typedef {import('./types.js').DealerAggregateStoreRecordQueryByAggregate} DealerAggregateStoreRecordQueryByAggregate
 * @typedef {import('./types.js').DealerAggregateStoreRecordQueryByStatus} DealerAggregateStoreRecordQueryByStatus
 * @typedef {import('./types.js').DealerAggregateStoreRecordStatus} DealerAggregateStoreRecordStatus
 */

/**
 * @param {AggregateRecord} record
 * @returns {DealerAggregateStoreRecord}
 */
const encodeRecord = (record) => {
  return {
    ...record,
    aggregate: record.aggregate.toString(),
    pieces: record.pieces.toString(),
    stat: encodeStatus(record.status)
  }
}

/**
 * @param {Partial<AggregateRecord>} record
 * @returns {Partial<DealerAggregateStoreRecord>}
 */
const encodePartialRecord = (record) => {
  return {
    ...(record.aggregate && { aggregate: record.aggregate.toString() }),
    ...(record.pieces && { pieces: record.pieces.toString() }),
    ...(record.status && { stat: encodeStatus(record.status) })
  }
}

/**
 * @param {AggregateStatus} status
 */
const encodeStatus = (status) => {
  switch (status) {
    case 'offered': {
      return Status.OFFERED
    }
    case 'accepted': {
      return Status.ACCEPTED
    }
    case 'invalid': {
      return Status.INVALID
    }
    default: {
      throw new Error('invalid status received for encoding')
    }
  }
}

/**
 * @param {AggregateRecordKey} recordKey
 * @returns {DealerAggregateStoreRecordKey}
 */
const encodeKey = (recordKey) => {
  return {
    aggregate: recordKey.aggregate.toString()
  }
}

/**
 * @param {AggregateRecordQuery} recordKey
 */
const encodeQueryProps = (recordKey) => {
  return {
    IndexName: 'stat',
    KeyConditions: {
      stat: {
        ComparisonOperator: 'EQ',
        AttributeValueList: [{ N: `${encodeStatus(recordKey.status)}` }]
      }
    }
  }
}

/**
 * @param {DealerAggregateStoreRecord} encodedRecord
 * @returns {AggregateRecord}
 */
export const decodeRecord = (encodedRecord) => {
  return {
    aggregate: parseLink(encodedRecord.aggregate),
    pieces: parseLink(encodedRecord.pieces),
    status: decodeStatus(encodedRecord.stat),
    insertedAt: encodedRecord.insertedAt,
    updatedAt: encodedRecord.updatedAt
  }
}

/**
 * @param {DealerAggregateStoreRecordStatus} status
 * @returns {"offered" | "accepted" | "invalid"}
 */
const decodeStatus = (status) => {
  switch (status) {
    case Status.OFFERED: {
      return 'offered'
    }
    case Status.ACCEPTED: {
      return 'accepted'
    }
    case Status.INVALID: {
      return 'invalid'
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
 * @returns {import('@storacha/filecoin-api/dealer/api').AggregateStore}
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
          /** @type {DealerAggregateStoreRecord} */ (unmarshall(res.Item))
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
        ':ua': { S: encodedRecord.updatedAt || new Date().toISOString() },
        ...(encodedRecord.stat && { ':st': { N: `${encodedRecord.stat}` } })
      }
      const stateUpdateExpression = encodedRecord.stat ? ', stat = :st' : ''
      const UpdateExpression = `SET updatedAt = :ua ${stateUpdateExpression}`

      const updateCmd = new UpdateItemCommand({
        TableName: context.tableName,
        Key: marshall(encodeKey(key)),
        UpdateExpression,
        ExpressionAttributeValues,
        ReturnValues: 'ALL_NEW'
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
          error: new StoreOperationFailed(
            'Missing `Attributes` property on DyanmoDB response'
          )
        }
      }

      return {
        ok: decodeRecord(
          /** @type {DealerAggregateStoreRecord} */ (unmarshall(res.Attributes))
        )
      }
    },
    query: async (search, options) => {
      const queryProps = encodeQueryProps(search)
      if (!queryProps) {
        return {
          error: new StoreOperationFailed(
            'no valid search parameters provided'
          )
        }
      }

      // @ts-ignore query props partial
      const queryCmd = new QueryCommand({
        TableName: context.tableName,
        ...queryProps,
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
            decodeRecord(
              /** @type {DealerAggregateStoreRecord} */ (unmarshall(item))
            )
          ),
          ...(res.LastEvaluatedKey
            ? { cursor: JSON.stringify(res.LastEvaluatedKey) }
            : {})
        }
      }
    }
  }
}

export const Status = {
  OFFERED: 0,
  ACCEPTED: 1,
  INVALID: 2
}
