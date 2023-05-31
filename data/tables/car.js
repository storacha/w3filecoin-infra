import {
  DynamoDBClient,
  BatchGetItemCommand,
  TransactWriteItemsCommand,
} from '@aws-sdk/client-dynamodb'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'

import {
  MAX_TRANSACT_WRITE_ITEMS,
  MAX_BATCH_GET_ITEMS
} from './constants.js'

/**
 * @typedef {import('../types').CarItem} CarItem
 */

/**
 * Abstraction layer to handle operations on Filecoin pending deal Car Table.
 *
 * @param {string} region
 * @param {string} tableName
 * @param {object} [options]
 * @param {string} [options.endpoint]
 * @returns {import('../types').CarTable}
 */
export function createCarTable (region, tableName, options = {}) {
  const dynamoDb = new DynamoDBClient({
    region,
    endpoint: options.endpoint
  })

  return {
    batchGet: async (cars) => {
      if (cars.length > MAX_BATCH_GET_ITEMS) {
        throw new RangeError('maximum batch size exceeded')
      }
      const cmd = new BatchGetItemCommand({
        RequestItems: {
          [tableName]: {
            Keys: cars.map(car => ({
              link: { S: car.link }
            }))
          }
        }
      })

      const response = await dynamoDb.send(cmd)
      if (!response.Responses) {
        return []
      }

      return response.Responses[tableName].map(item => /** @type {CarItem} */ (unmarshall(item)))
    },
    batchWrite: async (cars) => {
      if (cars.length > MAX_TRANSACT_WRITE_ITEMS) {
        throw new RangeError('maximum batch size exceeded')
      }

      const insertedAt = new Date().toISOString()
      const cmd = new TransactWriteItemsCommand({
        TransactItems: cars.map(car => ({
          Put: {
            TableName: tableName,
            Key: marshall({
              link: car.link
            }),
            Item: {
              link: { 'S': car.link },
              size: { 'N': `${car.size}`},
              src: { 'SS': car.src },
              commitmentProof: { 'S': car.commitmentProof },
              insertedAt: { 'S': insertedAt },
            },
            ConditionExpression: 'attribute_not_exists(link)',
          }
        })),
      })

      await dynamoDb.send(cmd)
    }
  }
}
