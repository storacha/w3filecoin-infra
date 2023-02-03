import {
  DynamoDBClient,
  UpdateItemCommand,
  QueryCommand,
  TransactWriteItemsCommand,
} from '@aws-sdk/client-dynamodb'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'

import { MAX_TRANSACT_WRITE_ITEMS } from './constants.js'

/**
 * @typedef {import('../types').FerryOpts} FerryOpts
 * @typedef {import('../types').FerryState} FerryState
 * @typedef {import('../types').FerryTable} FerryTable
 * @typedef {import('../types').CarItemFerry} CarItem
 */

/** @type {Record<string, FerryState>} */
export const FERRY_STATE = {
  loading: 'LOADING',
  ready: 'READY',
  dealPending: 'DEAL_PENDING',
  dealProcessed: 'DEAL_PROCESSED'
}

const MAX_SIZE = 127*(1<<28)
const MIN_SIZE = 1+127*(1<<27)


/**
 * Abstraction layer to handle operations on ferries with cargo to deliver to Spade.
 *
 * @param {string} region
 * @param {string} tableName
 * @param {FerryOpts} [options]
 * @returns {import('../types').FerryTable}
 */
export function createFerryTable (region, tableName, options = {}) {
  const dynamoDb = new DynamoDBClient({
    region,
    endpoint: options.endpoint
  })
  const minSize = options.minSize || MIN_SIZE
  const maxSize = options.maxSize || MAX_SIZE
  const cargoTableName = options.cargoTableName

  return {
    /**
     * Add given CARs as cargo to a ferry (if given ferry has still enough space).
     *
     * @param {string} id
     * @param {CarItem[]} cars 
     */
    addCargo: async (id, cars) => {
      if (cars.length > MAX_TRANSACT_WRITE_ITEMS - 1) {
        throw new RangeError('maximum batch size exceeded')
      }

      const links = cars.map(car => car.link)
      const updateAccumSize = cars.reduce((acc, car) => acc + car.size, 0)
      const maxSizeBeforeUpdate = maxSize - updateAccumSize

      if (maxSizeBeforeUpdate < 0) {
        throw new RangeError('Given CARs exceed maximum ferry size for given ferry')
      }

      const insertedAt = new Date().toISOString()
      const cmd = new TransactWriteItemsCommand({
        TransactItems: [
          // Add mapping of CARs to ferry
          ...links.map(link => ({
            Put: {
              TableName: cargoTableName,
              Item: {
                link: { 'S': link },
                ferryId: { 'S': id },
              },
            }
          })),
          {
            Update: {
              TableName: tableName,
              Key: marshall({
                id
              }),
              ExpressionAttributeValues: {
                ':insertedAt': { S: insertedAt },
                ':updatedAt': { S: insertedAt },
                ':loadingStat': { S: FERRY_STATE.loading },
                ':updateAccumSize': { N: `${updateAccumSize}` },
                ':maxSizeBeforeUpdate': { N: `${maxSizeBeforeUpdate}` }
              },
              // Condition expression runs before update. Guarantees that this operation only suceeds:
              // - when stat is loading
              // - when there is still enough space in the ferry for this batch size
              ConditionExpression: `
              (
                attribute_not_exists(stat) OR stat = :loadingStat
              )
              AND
              (
                attribute_not_exists(size) OR size <= :maxSizeBeforeUpdate
              )
              `,
              // Update row table with:
              // - insertedAt if row does not exist already
              // - updatedAt updated with current timestamp
              // - set state as loading if row does not exist already
              // - increment size
              UpdateExpression: `
              SET insertedAt = if_not_exists(insertedAt, :insertedAt),
                updatedAt = :updatedAt,
                stat = if_not_exists(stat, :loadingStat)
              ADD size :updateAccumSize
              `,
            }
          }
        ]
      })

      await dynamoDb.send(cmd)
    },
    /**
     * Get a ferry that is ready to load more CARs.
     */
    getFerryLoading: async () => {
      const queryCommand = new QueryCommand({
        TableName: tableName,
        IndexName: 'indexStat',
        Limit: 1,
        ExpressionAttributeValues: {
          ':loadingStat': { S: FERRY_STATE.loading },
        },
        KeyConditionExpression: 'stat = :loadingStat'
      })

      const res = await dynamoDb.send(queryCommand)
      if (res.Items?.length) {
        return unmarshall(res.Items[0]).id
      }
    },
    /**
     * Set given ferry as ready for a deal, not allowing any more data in.
     *
     * @param {string} id
     */
    setAsReady: async (id) => {
      const updatedAt = new Date().toISOString()

      const cmd = new UpdateItemCommand({
        TableName: tableName,
        Key: marshall({
          id
        }),
        ExpressionAttributeValues: {
          ':updatedAt': { S: updatedAt },
          ':loadingStat': { S: FERRY_STATE.loading },
          ':updateStat': { S: FERRY_STATE.ready },
          ':minSize': { N: `${minSize}` },
        },
        // Can only succeed when:
        // - ferry stat is in loading state
        // - size is enough
        ConditionExpression: `
        stat = :loadingStat AND size >= :minSize
        `,
        UpdateExpression: `
        SET updatedAt = :updatedAt,
            stat = :updateStat
        `,
      }) 

      await dynamoDb.send(cmd)
    },
    setAsDealPending: async (id) => {
      const updatedAt = new Date().toISOString()
      const cmd = new UpdateItemCommand({
        TableName: tableName,
        Key: marshall({
          id
        }),
        ExpressionAttributeValues: {
          ':updatedAt': { S: updatedAt },
          ':readyStat': { S: FERRY_STATE.ready },
          ':updateStat': { S: FERRY_STATE.dealPending },
        },
        // Can only succeed when ferry stat is in loading state
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
    setAsDealProcessed: async (id, commP) => {
      const updatedAt = new Date().toISOString()
      const cmd = new UpdateItemCommand({
        TableName: tableName,
        Key: marshall({
          id
        }),
        ExpressionAttributeValues: {
          ':updatedAt': { S: updatedAt },
          ':commP': { S: commP },
          ':pendingStat': { S: FERRY_STATE.dealPending },
          ':updateStat': { S: FERRY_STATE.dealProcessed },
        },
        // Can only succeed when ferry stat is in loading state
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
