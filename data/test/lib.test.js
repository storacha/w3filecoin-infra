import { test } from './helpers/context.js'
import { createDynamodDb, dynamoDBTableConfig } from './helpers/resources.js'
import { getCars } from './helpers/car.js'

import { CreateTableCommand, ScanCommand } from '@aws-sdk/client-dynamodb'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { customAlphabet } from 'nanoid'

import { aggregateTableProps, ferryTableProps } from '../tables/index.js'
import { AGGREGATE_STATE } from '../tables/aggregate.js'
import { addCarsToAggregate } from '../lib/add-cars-to-aggregate.js'
import { setAggregateAsReady } from '../lib/set-aggregate-as-ready.js'

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

test('can add cars to given aggregate', async t => {
  const { aggregateTableName, aggregateProps } = await getTable(t)
  const cars = await getCars(10)
  const { aggregateId } = await addCarsToAggregate(cars, aggregateProps)

  const aggregates = await getAggregates(t.context.dynamoClient, aggregateTableName)
  t.is(aggregates.length, 1)
  t.is(aggregates[0].aggregateId, aggregateId)
  t.is(aggregates[0].stat, AGGREGATE_STATE.ingesting)
})

test('can add cars to same aggregate', async t => {
  const { aggregateTableName, aggregateProps } = await getTable(t)
  const batches = await Promise.all([
    getCars(10),
    getCars(10)
  ])

  const { aggregateId: aggregateId0 } = await addCarsToAggregate(batches[0], aggregateProps)

  const aggregatesAfterFirstBatch = await getAggregates(t.context.dynamoClient, aggregateTableName)
  t.is(aggregatesAfterFirstBatch.length, 1)
  t.is(aggregatesAfterFirstBatch[0].aggregateId, aggregateId0)
  t.is(aggregatesAfterFirstBatch[0].stat, AGGREGATE_STATE.ingesting)

  const { aggregateId: aggregateId1 } = await addCarsToAggregate(batches[1], aggregateProps)
  t.is(aggregateId0, aggregateId1)

  const aggregatesAfterSecondBatch = await getAggregates(t.context.dynamoClient, aggregateTableName)
  t.is(aggregatesAfterSecondBatch.length, 1)
  t.is(aggregatesAfterSecondBatch[0].aggregateId, aggregateId1)
  t.is(aggregatesAfterSecondBatch[0].stat, AGGREGATE_STATE.ingesting)
})

test('can set an aggregate as ready', async t => {
  const { aggregateTableName, aggregateProps } = await getTable(t)
  const cars = await getCars(10)

  const { aggregateId } = await addCarsToAggregate(cars, aggregateProps)
  await setAggregateAsReady(aggregateId, aggregateProps)

  const aggregates = await getAggregates(t.context.dynamoClient, aggregateTableName)
  t.is(aggregates.length, 1)
  t.is(aggregates[0].aggregateId, aggregateId)
  t.is(aggregates[0].stat, AGGREGATE_STATE.ready)
})

test('can handle concurrent set of aggregate as ready', async t => {
  const { aggregateTableName, aggregateProps } = await getTable(t)
  const cars = await getCars(10)

  const { aggregateId } = await addCarsToAggregate(cars, aggregateProps)

  // Concurrent set aggregate as ready request
  await Promise.all([
    setAggregateAsReady(aggregateId, aggregateProps),
    setAggregateAsReady(aggregateId, aggregateProps)
  ])

  const aggregates = await getAggregates(t.context.dynamoClient, aggregateTableName)
  t.is(aggregates.length, 1)
  t.is(aggregates[0].aggregateId, aggregateId)
  t.is(aggregates[0].stat, AGGREGATE_STATE.ready)
})

test('can handle concurrent aggregates in ready state', async t => {
  let { aggregateTableName, aggregateProps } = await getTable(t)
  const batches = await Promise.all([
    getCars(10),
    getCars(10)
  ])

  // Simulare race condition
  // Attempt concurrent operations in new table until one exists
  let aggregatesResponses
  do {
    const table = await getTable(t)
    aggregateTableName = table.aggregateTableName
    aggregateProps = table.aggregateProps

    aggregatesResponses = await Promise.all(
      batches.map(batch => addCarsToAggregate(batch, table.aggregateProps))
    )
  } while (aggregatesResponses[0].aggregateId === aggregatesResponses[1].aggregateId)

  // Concurrent requests resulted in concurrent aggregates ingesting
  t.not(aggregatesResponses[0].aggregateId, aggregatesResponses[1].aggregateId)
  const concurrentIngestingAggregates = await getAggregates(t.context.dynamoClient, aggregateTableName)
  t.is(concurrentIngestingAggregates.length, 2)
  
  for (const aggregate of concurrentIngestingAggregates) {
    t.is(aggregate.stat, AGGREGATE_STATE.ingesting)
  }

  const moreBatches = await Promise.all([
    getCars(10),
    getCars(10)
  ])

  // Adds to one of the previous aggregates
  const { aggregateId: aggregateId0 } = await addCarsToAggregate(moreBatches[0], aggregateProps)
  t.truthy(aggregatesResponses.find(res => res.aggregateId === aggregateId0))
  
  // Adds to other previous aggregate when one finishes
  await setAggregateAsReady(aggregateId0, aggregateProps)
  const { aggregateId: aggregateId1 } = await addCarsToAggregate(moreBatches[0], aggregateProps)
  t.truthy(aggregatesResponses.find(res => res.aggregateId === aggregateId1))

  t.not(aggregateId0, aggregateId1)
})

/**
 * @param {import("ava").ExecutionContext<import("./helpers/context.js").AggregateContext>} t
 */
async function getTable (t) {
  const { aggregateTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const aggregateProps = {
    region: REGION,
    tableName: aggregateTableName,
    options: {
      ferryTableName,
      endpoint: t.context.dbEndpoint,
      minSize: 1
    }
  }

  return { aggregateProps, aggregateTableName }
}

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
  const [ aggregateTableName, ferryTableName ] = await Promise.all([
    createDynamoAggregateTable(dynamoClient),
    createDynamoFerryTable(dynamoClient),
  ])

  return {
    aggregateTableName,
    ferryTableName
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

/**
 * @param {import("@aws-sdk/client-dynamodb").DynamoDBClient} dynamo
 */
async function createDynamoFerryTable(dynamo) {
  const id = customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10)
  const tableName = id()

  await dynamo.send(new CreateTableCommand({
    TableName: tableName,
    ...dynamoDBTableConfig(ferryTableProps),
    ProvisionedThroughput: {
      ReadCapacityUnits: 1,
      WriteCapacityUnits: 1
    }
  }))

  return tableName
}
