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
import { CBOR } from '@ucanto/core'
import { parseLink } from '@ucanto/server'

import { connectTable } from './index.js'

/**
 * @typedef {import('@storacha/filecoin-api/types').StoreGetError} StoreGetError
 * @typedef {import('./types.js').AggregatorInclusionRecord} InclusionRecord
 * @typedef {import('@storacha/filecoin-api/aggregator/api').InclusionRecord} AggregatorInclusionRecord
 * @typedef {import('@storacha/filecoin-api/aggregator/api').InclusionRecordKey} InclusionRecordKey
 * @typedef {import('@storacha/filecoin-api/aggregator/api').InclusionRecordQueryByGroup} InclusionRecordQueryByGroup
 * @typedef {import('./types.js').AggregatorInclusionStoreRecord} InclusionStoreRecord
 * @typedef {Pick<InclusionStoreRecord, 'aggregate' | 'piece'>} InclusionStoreRecordKey
 */

/**
 * @param {InclusionRecord} record
 * @returns {InclusionStoreRecord}
 */
const encodeRecord = (record) => {
  return {
    ...record,
    aggregate: record.aggregate.toString(),
    piece: record.piece.toString(),
    inclusion: record.inclusion.toString()
  }
}

/**
 * @param {InclusionRecordKey} recordKey
 * @returns {InclusionStoreRecordKey}
 */
const encodeKey = (recordKey) => {
  return {
    aggregate: recordKey.aggregate.toString(),
    piece: recordKey.piece.toString()
  }
}

/**
 * @param {InclusionRecordQueryByGroup} recordSearch
 */
const encodeQueryProps = (recordSearch) => {
  return {
    IndexName: 'indexPiece',
    KeyConditions: {
      piece: {
        ComparisonOperator: 'EQ',
        AttributeValueList: [{ S: recordSearch.piece.toString() }]
      },
      group: {
        ComparisonOperator: 'EQ',
        AttributeValueList: [{ S: recordSearch.group }]
      }
    }
  }
}

/**
 * @param {InclusionStoreRecord} encodedRecord
 * @returns {InclusionRecord}
 */
export const decodeRecord = (encodedRecord) => {
  return {
    ...encodedRecord,
    aggregate: parseLink(encodedRecord.aggregate),
    piece: parseLink(encodedRecord.piece),
    inclusion: parseLink(encodedRecord.inclusion)
  }
}

/**
 * @param {import('./types.js').TableConnect | import('@aws-sdk/client-dynamodb').DynamoDBClient} conf
 * @param {object} context
 * @param {string} context.tableName
 * @param {import('./types.js').InclusionProofStore} context.inclusionProofStore
 * @returns {import('@storacha/filecoin-api/aggregator/api').InclusionStore}
 */
export function createClient (conf, context) {
  const tableclient = connectTable(conf)

  return {
    put: async (record) => {
      const inclusionBlock = await CBOR.write(record.inclusion)
      const inclusionProofPut = await context.inclusionProofStore.put({
        block: inclusionBlock.cid,
        inclusion: record.inclusion
      })

      if (inclusionProofPut.error) {
        return inclusionProofPut
      }

      const putCmd = new PutItemCommand({
        TableName: context.tableName,
        Item: marshall(
          encodeRecord({
            ...record,
            inclusion: inclusionBlock.cid
          }),
          {
            removeUndefinedValues: true
          }
        )
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

      return getInclusionRecordFromInclusionStoreRecord(
        /** @type {InclusionStoreRecord} */ (unmarshall(res.Item)),
        context.inclusionProofStore
      )
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

      const inclusionRecordsGet = await Promise.all(
        (res.Items ?? []).map((item) =>
          getInclusionRecordFromInclusionStoreRecord(
            /** @type {InclusionStoreRecord} */ (unmarshall(item)),
            context.inclusionProofStore
          )
        )
      )

      const records = []
      for (const get of inclusionRecordsGet) {
        if (get.error) {
          return get
        }
        records.push(get.ok)
      }

      return {
        ok: {
          results: records,
          ...(res.LastEvaluatedKey
            ? { cursor: JSON.stringify(res.LastEvaluatedKey) }
            : {})
        }
      }
    }
  }
}

/**
 * @param {import('./types.js').AggregatorInclusionStoreRecord} inclusionStoreRecord
 * @param {import('./types.js').InclusionProofStore} inclusionProofStore
 * @returns {Promise<import('../types.js').Result<AggregatorInclusionRecord, StoreGetError>>}
 */
async function getInclusionRecordFromInclusionStoreRecord (
  inclusionStoreRecord,
  inclusionProofStore
) {
  // Decode record and read inclusion proof
  const inclusionRecordWithInclusionLink = decodeRecord(inclusionStoreRecord)
  // Get inclusion proof
  const inclusionProofGet = await inclusionProofStore.get(
    inclusionRecordWithInclusionLink.inclusion
  )
  if (inclusionProofGet.error) {
    return inclusionProofGet
  }
  // Stick inclusion proof in record
  return {
    ok: {
      ...inclusionRecordWithInclusionLink,
      inclusion: inclusionProofGet.ok.inclusion
    }
  }
}
