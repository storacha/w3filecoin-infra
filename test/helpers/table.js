import { GetItemCommand, QueryCommand } from '@aws-sdk/client-dynamodb'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'
import pRetry from 'p-retry'

/**
 * @param {import('@aws-sdk/client-dynamodb').DynamoDBClient} dynamo
 * @param {string} tableName
 * @param {object} key
 */
export async function waitForTableItem (dynamo, tableName, key) {
  const cmd = new GetItemCommand({
    TableName: tableName,
    Key: marshall(key)
  })

  const response = await pRetry(async () => {
    const r = await dynamo.send(cmd)
    if (r.$metadata.httpStatusCode === 404) {
      throw new Error('not found in dynamoDB yet')
    }
    return r
  }, {
    maxTimeout: 1000,
    minTimeout: 500
  })

  return response.Item && unmarshall(response.Item)
}

/**
 * @param {import('@aws-sdk/client-dynamodb').DynamoDBClient} dynamo
 * @param {string} tableName
 * @param {Record<string, import('@aws-sdk/client-dynamodb').Condition>} keyConditions
 * @param {object} [options]
 * @param {string} [options.indexName]
 */
export async function waitForTableItems (dynamo, tableName, keyConditions, options = {}) {
  const cmd = new QueryCommand({
    TableName: tableName,
    KeyConditions: keyConditions,
    IndexName: options.indexName,
  })

  let response
  try {
    response = await pRetry(async () => {
      const r = await dynamo.send(cmd)
      if (r.$metadata.httpStatusCode === 404) {
        throw new Error('not found in dynamoDB yet')
      }
      return r
    }, {
      maxTimeout: 1000,
      minTimeout: 500
    })
  } catch {}

  return response?.Items && response?.Items.map(i => unmarshall(i))
}