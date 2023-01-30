import { testAggregate as test } from '../helpers/context.js'
import { createDynamodDb, dynamoDBTableConfig } from '../helpers/resources.js'
import { getCars } from '../helpers/car.js'

import { CreateTableCommand, ScanCommand } from '@aws-sdk/client-dynamodb'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { customAlphabet } from 'nanoid'

import { carTableProps } from '../../tables/index.js'
import { createCarTable } from '../../tables/car.js'

const REGION = 'us-west-2'

test.before(async t => {
  // Dynamo DB
  const {
    client: dynamo,
    endpoint: dbEndpoint
  } = await createDynamodDb({ port: 8000 })

  t.context.dbEndpoint = dbEndpoint
  t.context.dynamoClient = dynamo
})

test('can add cars to table uniquely', async (t) => {
  const { tableName } = await prepareResources(t.context.dynamoClient)
  t.is(tableName, tableName)

  const carTable = createCarTable(REGION, tableName, {
    endpoint: t.context.dbEndpoint
  })

  const cars = (await getCars(10)).map(car => ({
    link: car.link.toString(),
    size: car.size,
    commP: 'commP',
    url: 'url',
    md5: 'md5',
  }))

  await carTable.batchWrite(cars)

  await t.throwsAsync(() => carTable.batchWrite(cars))

  const carItems = await getCarItems(t.context.dynamoClient, tableName)
  t.is(carItems.length, cars.length)
  for (const carItem of carItems) {
    t.truthy(cars.find(car => car.link === carItem.link))
  }
})

/**
 * @param {import("@aws-sdk/client-dynamodb").DynamoDBClient} dynamo
 * @param {string} tableName
 * @param {object} [options]
 * @param {number} [options.limit]
 */
async function getCarItems (dynamo, tableName, options = {}) {
  const cmd = new ScanCommand({
    TableName: tableName,
    Limit: options.limit || 30
  })

  const response = await dynamo.send(cmd)
  return response.Items?.map(i => unmarshall(i)) || []
}

/**
 * @param {import("@aws-sdk/client-dynamodb").DynamoDBClient} dynamoClient
 */
async function prepareResources (dynamoClient) {
  const [ tableName ] = await Promise.all([
    createDynamoCarTable(dynamoClient),
  ])

  return {
    tableName
  }
}

/**
 * @param {import("@aws-sdk/client-dynamodb").DynamoDBClient} dynamo
 */
 async function createDynamoCarTable(dynamo) {
  const id = customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10)
  const tableName = id()

  await dynamo.send(new CreateTableCommand({
    TableName: tableName,
    ...dynamoDBTableConfig(carTableProps),
    ProvisionedThroughput: {
      ReadCapacityUnits: 1,
      WriteCapacityUnits: 1
    }
  }))

  return tableName
}
