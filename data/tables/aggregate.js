import {
  DynamoDBClient,
  UpdateItemCommand,
} from '@aws-sdk/client-dynamodb'
import { marshall } from '@aws-sdk/util-dynamodb'

/**
 * @typedef {import('../types').AggregateStat} AggregateStat
 * @typedef {import('../types').AggregateTable} AggregateTable
 * @typedef {import('../types').CarItemAggregate} CarItem
 */

/** @type {Record<string, AggregateStat>} */
export const AGGREGATE_STAT = {
  ingesting: 'INGESTING',
  ready: 'READY',
  dealPending: 'DEAL_PENDING',
  dealProcessed: 'DEAL_PROCESSED'
}

const DEFAULT_MAX_SIZE = 127*(1<<28)
const DEFAULT_MIN_SIZE = 1+127*(1<<27)

/**
 * Abstraction layer to handle operations for aggregates to send to Spade.
 *
 * @param {string} region
 * @param {string} tableName
 * @param {object} [options]
 * @param {string} [options.endpoint]
 * @param {number} [options.maxSize]
 * @param {number} [options.minSize]
 * @returns {import('../types').AggregateTable}
 */
export function createAggregateTable (region, tableName, options = {}) {
  const dynamoDb = new DynamoDBClient({
    region,
    endpoint: options.endpoint
  })
  const minSize = options.minSize || DEFAULT_MIN_SIZE
  const maxSize = options.maxSize || DEFAULT_MAX_SIZE

  return {
    /**
     * Add new CARs to the given aggregate if still with enough space.
     *
     * @param {string} aggregateId
     * @param {CarItem[]} cars 
     */
    add: async (aggregateId, cars) => {
      const links = cars.map(car => car.link)
      const updateAccumSize = cars.reduce((acc, car) => acc + car.size, 0)
      const maxSizeBeforeUpdate = maxSize - updateAccumSize

      if (maxSizeBeforeUpdate < 0) {
        throw new Error('TODO error')
      }

      const insertedAt = new Date().toISOString()

      const updateItemcommand = new UpdateItemCommand({
        TableName: tableName,
        Key: marshall({
          aggregateId
        }),
        ExpressionAttributeValues: {
          ':insertedAt': { S: insertedAt },
          ':updatedAt': { S: insertedAt },
          ':initialStat': { S: AGGREGATE_STAT.ingesting },
          ':initialSize': { N: `${0}` },
          ':updateAccumSize': { N: `${updateAccumSize}` },
          ':maxSizeBeforeUpdate': { N: `${maxSizeBeforeUpdate}` },
          ':links' :{ SS: links } // SS is "String Set"
        },
        // Condition expression runs before update. Guarantees that this operation only suceeds:
        // - when stat is ingesting
        // - when there is still enough space in the aggregate for this batch size
        ConditionExpression: `
        (
          attribute_not_exists(stat) OR stat = :initialStat
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
          stat = if_not_exists(stat, :initialStat),
          size = if_not_exists(size, :initialSize) + :updateAccumSize
        ADD cars :links
        `,
      })

      await dynamoDb.send(updateItemcommand)
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
          ':initialStat': { S: AGGREGATE_STAT.ingesting },
          ':updateStat': { S: AGGREGATE_STAT.ready },
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
          ':readyStat': { S: AGGREGATE_STAT.ready },
          ':updateStat': { S: AGGREGATE_STAT.dealPending },
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
    closeAggregate: async (aggregateId, commP) => {
      const updatedAt = new Date().toISOString()
      const cmd = new UpdateItemCommand({
        TableName: tableName,
        Key: marshall({
          aggregateId
        }),
        ExpressionAttributeValues: {
          ':updatedAt': { S: updatedAt },
          ':commP': { S: commP },
          ':pendingStat': { S: AGGREGATE_STAT.dealPending },
          ':updateStat': { S: AGGREGATE_STAT.dealProcessed },
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
