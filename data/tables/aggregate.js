import {
  DynamoDBClient,
  UpdateItemCommand,
  QueryCommand,
  TransactWriteItemsCommand,
} from '@aws-sdk/client-dynamodb'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'

import { MAX_TRANSACT_WRITE_ITEMS } from './constants.js'

/**
 * @typedef {import('../types').AggregateOpts} AggregateOpts
 * @typedef {import('../types').AggregateState} AggregateState
 * @typedef {import('../types').AggregateTable} AggregateTable
 * @typedef {import('../types').CarItemAggregate} CarItem
 */

/** @type {Record<string, AggregateState>} */
export const AGGREGATE_STATE = {
  ingesting: 'INGESTING',
  ready: 'READY',
  dealPending: 'DEAL_PENDING',
  dealProcessed: 'DEAL_PROCESSED'
}

const MAX_SIZE = 127*(1<<28)
const MIN_SIZE = 1+127*(1<<27)


/**
 * Abstraction layer to handle operations for aggregates to send to Spade.
 *
 * @param {string} region
 * @param {string} tableName
 * @param {AggregateOpts} [options]
 * @returns {import('../types').AggregateTable}
 */
export function createAggregateTable (region, tableName, options = {}) {
  const dynamoDb = new DynamoDBClient({
    region,
    endpoint: options.endpoint
  })
  const minSize = options.minSize || MIN_SIZE
  const maxSize = options.maxSize || MAX_SIZE
  const ferryTableName = options.ferryTableName

  return {
    /**
     * Append given CARs to the given aggregate if still with enough space.
     *
     * @param {string} aggregateId
     * @param {CarItem[]} cars 
     */
    appendCARs: async (aggregateId, cars) => {
      if (cars.length > MAX_TRANSACT_WRITE_ITEMS - 1) {
        throw new RangeError('maximum batch size exceeded')
      }

      const links = cars.map(car => car.link)
      const updateAccumSize = cars.reduce((acc, car) => acc + car.size, 0)
      const maxSizeBeforeUpdate = maxSize - updateAccumSize

      if (maxSizeBeforeUpdate < 0) {
        throw new RangeError('Given CARs exceed maximum aggregate size for given aggregate')
      }

      const insertedAt = new Date().toISOString()
      const cmd = new TransactWriteItemsCommand({
        TransactItems: [
          // Add mapping of CARs to aggregate
          ...links.map(link => ({
            Put: {
              TableName: ferryTableName,
              Item: {
                link: { 'S': link },
                aggregateId: { 'S': aggregateId },
              },
            }
          })),
          {
            Update: {
              TableName: tableName,
              Key: marshall({
                aggregateId
              }),
              ExpressionAttributeValues: {
                ':insertedAt': { S: insertedAt },
                ':updatedAt': { S: insertedAt },
                ':ingestingStat': { S: AGGREGATE_STATE.ingesting },
                ':updateAccumSize': { N: `${updateAccumSize}` },
                ':maxSizeBeforeUpdate': { N: `${maxSizeBeforeUpdate}` }
              },
              // Condition expression runs before update. Guarantees that this operation only suceeds:
              // - when stat is ingesting
              // - when there is still enough space in the aggregate for this batch size
              ConditionExpression: `
              (
                attribute_not_exists(stat) OR stat = :ingestingStat
              )
              AND
              (
                attribute_not_exists(size) OR size <= :maxSizeBeforeUpdate
              )
              `,
              // Update row table with:
              // - insertedAt if row does not exist already
              // - updatedAt updated with current timestamp
              // - set state as ingesting if row does not exist already
              // - increment size
              UpdateExpression: `
              SET insertedAt = if_not_exists(insertedAt, :insertedAt),
                updatedAt = :updatedAt,
                stat = if_not_exists(stat, :ingestingStat)
              ADD size :updateAccumSize
              `,
            }
          }
        ]
      })

      await dynamoDb.send(cmd)
    },
    /**
     * Get an aggregate that is ready to track more CARs.
     */
    getAggregateIngesting: async () => {
      const queryCommand = new QueryCommand({
        TableName: tableName,
        IndexName: 'indexStat',
        Limit: 1,
        ExpressionAttributeValues: {
          ':ingestingStat': { S: AGGREGATE_STATE.ingesting },
        },
        KeyConditionExpression: 'stat = :ingestingStat'
      })

      const res = await dynamoDb.send(queryCommand)
      if (res.Items?.length) {
        return unmarshall(res.Items[0]).aggregateId
      }
    },
    /**
     * Set given aggregate as ready for a deal, not allowing any more data in.
     *
     * @param {string} aggregateId
     */
    setAsReady: async (aggregateId) => {
      const updatedAt = new Date().toISOString()

      const cmd = new UpdateItemCommand({
        TableName: tableName,
        Key: marshall({
          aggregateId
        }),
        ExpressionAttributeValues: {
          ':updatedAt': { S: updatedAt },
          ':initialStat': { S: AGGREGATE_STATE.ingesting },
          ':updateStat': { S: AGGREGATE_STATE.ready },
          ':minSize': { N: `${minSize}` },
        },
        // Can only succeed when:
        // - aggregate stat is in ingesting state
        // - size is enough
        ConditionExpression: `
        stat = :initialStat AND size >= :minSize
        `,
        UpdateExpression: `
        SET updatedAt = :updatedAt,
            stat = :updateStat
        `,
      }) 

      await dynamoDb.send(cmd)
    },
    setAsDealPending: async (aggregateId) => {
      const updatedAt = new Date().toISOString()
      const cmd = new UpdateItemCommand({
        TableName: tableName,
        Key: marshall({
          aggregateId
        }),
        ExpressionAttributeValues: {
          ':updatedAt': { S: updatedAt },
          ':readyStat': { S: AGGREGATE_STATE.ready },
          ':updateStat': { S: AGGREGATE_STATE.dealPending },
        },
        // Can only succeed when aggregate stat is in Ingesting state
        ConditionExpression: `
          stat = :readyStat
        `,
        UpdateExpression: `
        SET updatedAt = :updatedAt,
            stat = :updateStat
        `,
      }) 

      await dynamoDb.send(cmd)
    },
    setAsDealProcessed: async (aggregateId, commP) => {
      const updatedAt = new Date().toISOString()
      const cmd = new UpdateItemCommand({
        TableName: tableName,
        Key: marshall({
          aggregateId
        }),
        ExpressionAttributeValues: {
          ':updatedAt': { S: updatedAt },
          ':commP': { S: commP },
          ':pendingStat': { S: AGGREGATE_STATE.dealPending },
          ':updateStat': { S: AGGREGATE_STATE.dealProcessed },
        },
        // Can only succeed when aggregate stat is in Ingesting state
        ConditionExpression: `
          stat = :pendingStat
        `,
        UpdateExpression: `
        SET updatedAt = :updatedAt,
            stat = :updateStat,
            commP = :commP
        `,
      }) 

      await dynamoDb.send(cmd)
    },
  }
}
