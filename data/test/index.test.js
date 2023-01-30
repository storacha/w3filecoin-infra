import { test } from './helpers/context.js'
import { createRedisDb, createDynamodDb, dynamoDBTableConfig } from './helpers/resources.js'
import { getCars } from './helpers/car.js'

import { CreateTableCommand, ScanCommand } from '@aws-sdk/client-dynamodb'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { customAlphabet } from 'nanoid'

import { aggregateTableProps } from '../tables/index.js'
import { AGGREGATE_STAT } from '../tables/aggregate.js'
import { addCarsToAggregate, setAggregateAsReady } from '../index.js'
import { createRedis } from '../redis.js'

const REGION = 'us-west-2'

test.before(async t => {
  // Dynamo DB
  const {
    client: dynamo,
    endpoint: dbEndpoint
  } = await createDynamodDb({ port: 8000 })

  // Redis DB
  const {
    endpoint: redisEndpoint,
    port: redisPort
  } = await createRedisDb()

  t.context.dbEndpoint = dbEndpoint
  t.context.dynamoClient = dynamo

  t.context.redisEndpoint = redisEndpoint
  t.context.redisPort = redisPort
})

test('can add cars to given aggregate', async t => {
  const { tableName, redisKey } = await prepareResources(t.context.dynamoClient)
  const redis = createRedis(t.context.redisEndpoint, t.context.redisPort)
  const aggregateId = `${Date.now()}`
  await redis.set(redisKey, aggregateId)

  const cars = await getCars(10)
  const aggregateProps = {
    region: REGION,
    tableName,
    options: {
      endpoint: t.context.dbEndpoint
    }
  }
  const redisProps = {
    url: t.context.redisEndpoint,
    port: t.context.redisPort,
    key: redisKey
  }

  await addCarsToAggregate(cars, aggregateProps, redisProps)

  const aggregates = await getAggregates(t.context.dynamoClient, tableName)
  t.is(aggregates.length, 1)
  t.is(aggregates[0].aggregateId, aggregateId)
  t.is(aggregates[0].stat, AGGREGATE_STAT.ingesting)
})

test('fails to add cars to non existent aggregate', async t => {
  const { tableName, redisKey } = await prepareResources(t.context.dynamoClient)

  const cars = await getCars(10)
  const aggregateProps = {
    region: REGION,
    tableName,
    options: {
      endpoint: t.context.dbEndpoint
    }
  }
  const redisProps = {
    url: t.context.redisEndpoint,
    port: t.context.redisPort,
    key: redisKey
  }

  await t.throwsAsync(() => addCarsToAggregate(cars, aggregateProps, redisProps))

  const aggregates = await getAggregates(t.context.dynamoClient, tableName)
  t.is(aggregates.length, 0)
})

test('can set an aggregate as ready', async t => {
  const { tableName, redisKey } = await prepareResources(t.context.dynamoClient)
  const redis = createRedis(t.context.redisEndpoint, t.context.redisPort)
  const aggregateId = `${Date.now()}`
  await redis.set(redisKey, aggregateId)

  const cars = await getCars(10)
  const aggregateProps = {
    region: REGION,
    tableName,
    options: {
      endpoint: t.context.dbEndpoint,
      minSize: 1
    }
  }
  const redisProps = {
    url: t.context.redisEndpoint,
    port: t.context.redisPort,
    key: redisKey
  }

  await addCarsToAggregate(cars, aggregateProps, redisProps)

  await setAggregateAsReady({ aggregateId }, aggregateProps, redisProps)

  const aggregates = await getAggregates(t.context.dynamoClient, tableName)
  t.is(aggregates.length, 1)
  t.is(aggregates[0].aggregateId, aggregateId)
  t.is(aggregates[0].stat, AGGREGATE_STAT.ready)
})

/**
 * @param {import("@aws-sdk/client-dynamodb").DynamoDBClient} dynamo
 * @param {string} tableName
 * @param {object} [options]
 * @param {number} [options.limit]
 */
async function getAggregates (dynamo, tableName, options = {}) {
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
  const id = customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10)
  // TODO: Fill in
  const [ tableName ] = await Promise.all([
    createDynamoAggregateTable(dynamoClient),
  ])

  return {
    tableName,
    redisKey: id()
  }
}

/**
 * @param {import("@aws-sdk/client-dynamodb").DynamoDBClient} dynamo
 */
async function createDynamoAggregateTable(dynamo) {
  const id = customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10)
  const tableName = id()

  await dynamo.send(new CreateTableCommand({
    TableName: tableName,
    ...dynamoDBTableConfig(aggregateTableProps),
    ProvisionedThroughput: {
      ReadCapacityUnits: 1,
      WriteCapacityUnits: 1
    }
  }))

  return tableName
}
