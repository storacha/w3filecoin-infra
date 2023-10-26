import { PutItemCommand, GetItemCommand, BatchWriteItemCommand, QueryCommand } from '@aws-sdk/client-dynamodb'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'
import { RecordNotFound, StoreOperationFailed } from '@web3-storage/filecoin-api/errors'
import { parseLink } from '@ucanto/server'

import { connectTable } from './index.js'

/**
 * @typedef {import('@web3-storage/data-segment').PieceLink} PieceLink
 * @typedef {'offered' | 'accepted' | 'invalid'} AggregateStatus
 * @typedef {import('@web3-storage/filecoin-api/dealer/api').AggregateRecord} AggregateRecord
 * @typedef {import('@web3-storage/filecoin-api/dealer/api').AggregateRecordKey} AggregateRecordKey
 * @typedef {{ status?: AggregateStatus, aggregate?: PieceLink }} AggregateRecordQuery
 * @typedef {import('./types').DealerAggregateStoreRecord} DealerAggregateStoreRecord
 * @typedef {import('./types').DealerAggregateStoreRecordKey} DealerAggregateStoreRecordKey
 * @typedef {import('./types').DealerAggregateStoreRecordQueryByAggregate} DealerAggregateStoreRecordQueryByAggregate
 * @typedef {import('./types').DealerAggregateStoreRecordQueryByStatus} DealerAggregateStoreRecordQueryByStatus
 * @typedef {import('./types').DealerAggregateStoreRecordStatus} DealerAggregateStoreRecordStatus
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
    // fallback to -1 given is key
    dealMetadataDealId: record.deal?.dataSource.dealID ? Number(record.deal?.dataSource.dealID) : -1,
    dealMetadataDataType: record.deal?.dataType !== undefined ? Number(record.deal?.dataType) : undefined,
    stat: encodeStatus(record.status)
  }
}

/**
 * @param {Partial<AggregateRecord>} record 
 * @returns {Partial<DealerAggregateStoreRecord>} 
 */
const encodePartialRecord = (record) => {
  return {
    ...(record.aggregate && { aggregate: record.aggregate?.toString() }),
    ...(record.pieces && { pieces: record.pieces?.toString() }),
    ...(record.status && { stat: encodeStatus(record.status) }),
    // fallback to -1 given is key
    dealMetadataDealId: record.deal?.dataSource.dealID ? Number(record.deal?.dataSource.dealID) : 0,
    dealMetadataDataType: record.deal?.dataType !== undefined ? Number(record.deal?.dataType) : undefined,
  }
}

/**
 * @param {AggregateStatus} status 
 */
const encodeStatus = (status) => {
  if (status === 'offered') {
    return Status.OFFERED
  } else if (status === 'accepted') {
    return Status.ACCEPTED
  }
  return Status.INVALID
}

/**
 * @param {AggregateRecordKey} recordKey 
 * @returns {DealerAggregateStoreRecordKey} 
 */
const encodeKey = (recordKey) => {
  return {
    aggregate: recordKey.aggregate.toString(),
    // fallback to -1 given is key
    dealMetadataDealId: recordKey.deal?.dataSource.dealID ? Number(recordKey.deal?.dataSource.dealID) : -1,
  }
}

/**
 * @param {AggregateRecordQuery} recordKey 
 */
const encodeQueryProps = (recordKey) => {
  if (recordKey.status) {
    return {
      IndexName: 'stat',
      KeyConditions: {
        stat: {
          ComparisonOperator: 'EQ',
          AttributeValueList: [{ N: `${encodeStatus(recordKey.status)}` }]
        }
      }
    }
  } else if (recordKey.aggregate) {
    return {
      IndexName: 'aggregate',
      KeyConditions: {
        aggregate: {
          ComparisonOperator: 'EQ',
          AttributeValueList: [{ S: `${recordKey.aggregate.toString()}` }]
        }
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
    deal: encodedRecord.dealMetadataDealId && encodedRecord.dealMetadataDealId !== -1 && encodedRecord.dealMetadataDataType !== undefined ?
      { dataType: BigInt(encodedRecord.dealMetadataDataType), dataSource: { dealID: BigInt(encodedRecord.dealMetadataDealId) } } :
      undefined,
    insertedAt: encodedRecord.insertedAt,
    updatedAt: encodedRecord.updatedAt
  }
}

/**
 * @param {DealerAggregateStoreRecordStatus} status 
 * @returns {"offered" | "accepted" | "invalid"}
 */
const decodeStatus = (status) => {
  if (status === Status.OFFERED) {
    return 'offered'
  } else if (status === Status.ACCEPTED) {
    return 'accepted'
  }
  return 'invalid'
}

/**
 * @param {import('./types.js').TableConnect | import('@aws-sdk/client-dynamodb').DynamoDBClient} conf
 * @param {object} context
 * @param {string} context.tableName
 * @returns {import('@web3-storage/filecoin-api/dealer/api').AggregateStore}
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
          /** @type {DealerAggregateStoreRecord} */ (unmarshall(res.Item))
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
      // Encode partial value with new properties
      const updatedValueDiff = encodePartialRecord(record)

      // Get current value
      const getCmd = new GetItemCommand({
        TableName: context.tableName,
        Key: marshall(encodeKey(key)),
      })
      let getRes
      try {
        getRes = await tableclient.send(getCmd)
      } catch (/** @type {any} */ error) {
        return {
          error: new StoreOperationFailed(error.message)
        }
      }
      // not found error
      if (!getRes.Item) {
        return {
          error: new RecordNotFound('item to update not found in store')
        }
      }

      // Create new value
      const currentValue = /** @type {DealerAggregateStoreRecord} */ (unmarshall(getRes.Item))
      const newValue = {
        ...currentValue,
        updatedAt: (new Date()).toISOString(),
        // overwrite new columns
        ...updatedValueDiff,
      }

      // DynamoDB does not allow update a key, so we need to put and delete record
      // so that we can guarantee uniqueness of aggregate, deal pairs
      const batchCommand = new BatchWriteItemCommand({
        RequestItems: {
          [context.tableName]: [
            {
              PutRequest: {
                Item: marshall(newValue, {
                  removeUndefinedValues: true
                }),
              }
            },
            {
              DeleteRequest: {
                Key: marshall(encodeKey(key)),
              }
            }
          ]
        }
      })

      try {
        await tableclient.send(batchCommand)
      } catch (/** @type {any} */ error) {
        return {
          error: new StoreOperationFailed(error.message)
        }
      }

      return {
        ok: decodeRecord(newValue)
      }
    },
    query: async (search) => {
      const queryProps = encodeQueryProps(search)
      if (!queryProps) {
        return {
          error: new StoreOperationFailed('no valid search parameters provided')
        }
      }

      // @ts-ignore query props partial
      const queryCmd = new QueryCommand({
        TableName: context.tableName,
        ...queryProps
      })

      let res
      try {
        res = await tableclient.send(queryCmd)
      } catch (/** @type {any} */ error) {
        return {
          error: new StoreOperationFailed(error.message)
        }
      }

      // TODO: handle pulling the entire list. Even with renewals we are far away from this being needed
      return {
        ok: res.Items ? res.Items.map(item => decodeRecord(
          /** @type {DealerAggregateStoreRecord} */ (unmarshall(item))
        )) : []
      }
    },
  }
}

export const Status = {
  OFFERED: 0,
  ACCEPTED: 1,
  INVALID: 2
}
