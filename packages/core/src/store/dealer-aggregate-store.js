import { PutItemCommand, GetItemCommand, UpdateItemCommand, QueryCommand } from '@aws-sdk/client-dynamodb'
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
    ...record,
    aggregate: record.aggregate?.toString(),
    pieces: record.pieces?.toString(),
    // fallback to -1 given is key
    dealMetadataDealId: record.deal?.dataSource.dealID ? Number(record.deal?.dataSource.dealID) : 0,
    dealMetadataDataType: record.deal?.dataType !== undefined ? Number(record.deal?.dataType) : undefined,
    stat: record.status && encodeStatus(record.status)
  }
}

/**
 * @param {AggregateStatus} status 
 */
const encodeStatus = (status) => {
  if (status === 'offered') {
    return 0
  } else if (status === 'accepted') {
    return 1
  }
  return 2
}

/**
 * @param {AggregateRecordKey} recordKey 
 * @returns {DealerAggregateStoreRecordKey} 
 */
const encodeKey = (recordKey) => {
  return {
    ...recordKey,
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
      IndexName: 'piece',
      KeyConditions: {
        piece: {
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
const decodeRecord = (encodedRecord) => {
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
  if (status === 0) {
    return 'offered'
  } else if (status === 1) {
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
      const encodedRecord = encodePartialRecord(record)
      const ExpressionAttributeValues = {
        ':ua': { S: encodedRecord.updatedAt || (new Date()).toISOString() },
        ...(encodedRecord.stat && {':st': { N: `${encodedRecord.stat}` }})
      }
      const stateUpdateExpression = encodedRecord.stat ? ', stat = "st' : ''
      const UpdateExpression = `SET updatedAt = :ua ${stateUpdateExpression}`

      const updateCmd = new UpdateItemCommand({
        TableName: context.tableName,
        Key: marshall(encodeKey(key)),
        UpdateExpression,
        ExpressionAttributeValues,
        ReturnValues: 'ALL',
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
          /** @type {DealerAggregateStoreRecord} */ (unmarshall(res.Attributes))
        )
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

      // TODO: handle pulling the entire list. currently we only support 2 providers so
      // this list should not be longer than the default page size so this is not terribly urgent.
      return {
        ok: res.Items ? res.Items.map(item => decodeRecord(
          /** @type {DealerAggregateStoreRecord} */ (unmarshall(item))
        )) : []
      }
    },
  }
}
