import { test } from '../helpers/context.js'
import { createDynamodDb, dynamoDBTableConfig } from '../helpers/resources.js'
import { getCars } from '../helpers/car.js'

import { CreateTableCommand, ScanCommand } from '@aws-sdk/client-dynamodb'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { customAlphabet } from 'nanoid'

import { aggregateTableProps, ferryTableProps } from '../../tables/index.js'
import { createAggregateTable, AGGREGATE_STATE } from '../../tables/aggregate.js'

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

test('can add multiple batches to same aggregate', async t => {
  const batchSize = 10
  const batchCount = 2
  const aggregateId = `${Date.now()}`

  const { aggregateTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const batches = await getBatchesToAggregate(batchCount, batchSize)
  const totalSizeToAggregate = batches.flat().reduce((accum, car) => accum + car.size, 0)

  const aggregateTable = createAggregateTable(REGION, aggregateTableName, {
    ferryTableName,
    endpoint: t.context.dbEndpoint,
    // Make max size enough for sum of all batches size
    maxSize: totalSizeToAggregate
  })

  // Insert batches into aggregate table
  for (const [i, batch] of batches.entries()) {
    await aggregateTable.appendCARs(aggregateId, batch)

    const aggregates = await getAggregates(t.context.dynamoClient, aggregateTableName)
    const carsInFerry = await getCarsInAggregateFerry(t.context.dynamoClient, ferryTableName)

    // only one aggregate exists all the time
    t.is(aggregates.length, 1)

    t.is(carsInFerry.length, batchSize + (i * batchSize))
    t.is(aggregates[0].stat, AGGREGATE_STATE.ingesting)
  }

  const aggregates = await getAggregates(t.context.dynamoClient, aggregateTableName)
  const carsInFerry = await getCarsInAggregateFerry(t.context.dynamoClient, ferryTableName)

  // only one aggregate exists all the time
  t.is(aggregates.length, 1)
  t.is(carsInFerry.length, batchSize * batchCount)
  t.is(aggregates[0].aggregateId, aggregateId)
  t.is(aggregates[0].stat, AGGREGATE_STATE.ingesting)
  t.is(aggregates[0].size, totalSizeToAggregate)
  t.truthy(aggregates[0].insertedAt)
  t.truthy(aggregates[0].updatedAt)
  t.not(aggregates[0].insertedAt, aggregates[0].updatedAt)

  // Validate all CARs in ferry are assigned to given aggregate
  for (const car of carsInFerry) {
    t.is(car.aggregateId, aggregateId)
  }
})

test('fails to insert a new batch if its size is bigger than max', async t => {
  const batchSize = 10
  const batchCount = 1
  const aggregateId = `${Date.now()}`

  const { aggregateTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const batches = await getBatchesToAggregate(batchCount, batchSize)
  const totalSizeToAggregate = batches.flat().reduce((accum, car) => accum + car.size, 0)

  const aggregateTable = createAggregateTable(REGION, aggregateTableName, {
    ferryTableName,
    endpoint: t.context.dbEndpoint,
    // Make max size smaller than requested content to aggregate
    maxSize: totalSizeToAggregate - 1
  })

  await t.throwsAsync(() => aggregateTable.appendCARs(aggregateId, batches[0]))

  const aggregates = await getAggregates(t.context.dynamoClient, aggregateTableName)
  // no aggregates exist
  t.is(aggregates.length, 0)
})

test('fails to insert a second batch if total size is bigger than max', async t => {
  const batchSize = 10
  const batchCount = 2
  const aggregateId = `${Date.now()}`

  const { aggregateTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const batches = await getBatchesToAggregate(batchCount, batchSize)
  const totalSizeToAggregate = batches.flat().reduce((accum, car) => accum + car.size, 0)

  const aggregateTable = createAggregateTable(REGION, aggregateTableName, {
    ferryTableName,
    endpoint: t.context.dbEndpoint,
    // Make max size smaller than requested content to aggregate
    maxSize: totalSizeToAggregate - 1
  })

  // First batch succeeds to insert
  await aggregateTable.appendCARs(aggregateId, batches[0])

  // Second batch fails to insert
  await t.throwsAsync(() => aggregateTable.appendCARs(aggregateId, batches[1]))

  const aggregates = await getAggregates(t.context.dynamoClient, aggregateTableName)
  const carsInFerry = await getCarsInAggregateFerry(t.context.dynamoClient, ferryTableName)
  
  t.is(aggregates.length, 1)
  t.is(carsInFerry.length, Number(batchSize) * 1)
  t.is(aggregates[0].stat, AGGREGATE_STATE.ingesting)
  t.is(aggregates[0].aggregateId, aggregateId)
  t.is(aggregates[0].size, batches[0].reduce((accum, car) => accum + car.size, 0))
  t.truthy(aggregates[0].insertedAt)
  t.truthy(aggregates[0].updatedAt)
  t.is(aggregates[0].insertedAt, aggregates[0].updatedAt)
})

test('can transition aggregate states', async t => {
  const batchSize = 10
  const batchCount = 1
  const aggregateId = `${Date.now()}`

  const { aggregateTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const batches = await getBatchesToAggregate(batchCount, batchSize)
  const totalSizeToAggregate = batches.flat().reduce((accum, car) => accum + car.size, 0)

  const aggregateTable = createAggregateTable(REGION, aggregateTableName, {
    ferryTableName,
    endpoint: t.context.dbEndpoint,
    minSize: totalSizeToAggregate / 2,
    maxSize: totalSizeToAggregate
  })

  // Insert batches into aggregate table
  for (const batch of batches) {
    await aggregateTable.appendCARs(aggregateId, batch)
  }

  const aggregatesBeforeLock = await getAggregates(t.context.dynamoClient, aggregateTableName)
  // only one aggregate exists all the time
  t.is(aggregatesBeforeLock.length, 1)
  t.is(aggregatesBeforeLock[0].aggregateId, aggregateId)
  t.is(aggregatesBeforeLock[0].stat, AGGREGATE_STATE.ingesting)

  // set aggregate as ready
  await aggregateTable.setAsReady(aggregateId)

  const aggregatesAfterLock = await getAggregates(t.context.dynamoClient, aggregateTableName)
  // only one aggregate exists all the time
  t.is(aggregatesAfterLock.length, 1)
  t.is(aggregatesAfterLock[0].stat, AGGREGATE_STATE.ready)

  // set aggregate as pending deal
  await aggregateTable.setAsDealPending(aggregatesAfterLock[0].aggregateId)

  const aggregatesPendingDeal = await getAggregates(t.context.dynamoClient, aggregateTableName)
  // only one aggregate exists all the time
  t.is(aggregatesPendingDeal.length, 1)
  t.is(aggregatesPendingDeal[0].aggregateId, aggregateId)
  t.is(aggregatesPendingDeal[0].stat, AGGREGATE_STATE.dealPending)
  t.falsy(aggregatesPendingDeal[0].commP)

  // set aggregate as deal processed
  const commP = 'commP...a'
  await aggregateTable.setAsDealProcessed(aggregatesAfterLock[0].aggregateId, commP)

  const aggregatesProcessedDeal = await getAggregates(t.context.dynamoClient, aggregateTableName)
  // only one aggregate exists all the time
  t.is(aggregatesProcessedDeal.length, 1)
  t.is(aggregatesProcessedDeal[0].aggregateId, aggregateId)
  t.is(aggregatesProcessedDeal[0].stat, AGGREGATE_STATE.dealProcessed)
  t.is(aggregatesProcessedDeal[0].commP, commP)
})

test('fails to lock aggregate without a minimum size', async t => {
  const batchSize = 10
  const batchCount = 2
  const aggregateId = `${Date.now()}`

  const { aggregateTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const batches = await getBatchesToAggregate(batchCount, batchSize)
  const totalSizeToAggregate = batches.flat().reduce((accum, car) => accum + car.size, 0)

  const aggregateTable = createAggregateTable(REGION, aggregateTableName, {
    ferryTableName,
    endpoint: t.context.dbEndpoint,
    // Needs more than one batch minimum
    minSize: (totalSizeToAggregate / 2) + 1,
    maxSize: totalSizeToAggregate
  })

  // First batch succeeds to insert
  await aggregateTable.appendCARs(aggregateId, batches[0])

  await t.throwsAsync(() => aggregateTable.setAsReady(aggregateId))

  const aggregatesBeforeLock = await getAggregates(t.context.dynamoClient, aggregateTableName)
  // only one aggregate exists all the time
  t.is(aggregatesBeforeLock.length, 1)
  t.is(aggregatesBeforeLock[0].aggregateId, aggregateId)
  t.is(aggregatesBeforeLock[0].stat, AGGREGATE_STATE.ingesting)

  // First batch succeeds to insert
  await aggregateTable.appendCARs(aggregateId, batches[1])

  await aggregateTable.setAsReady(aggregateId)
  const aggregatesAfterLock = await getAggregates(t.context.dynamoClient, aggregateTableName)
  // only one aggregate exists all the time
  t.is(aggregatesAfterLock.length, 1)
  t.is(aggregatesAfterLock[0].aggregateId, aggregateId)
  t.is(aggregatesAfterLock[0].stat, AGGREGATE_STATE.ready)
})

test('fails to set as deal pending when aggregate not ready', async t => {
  const batchSize = 10
  const batchCount = 1
  const aggregateId = `${Date.now()}`

  const { aggregateTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const batches = await getBatchesToAggregate(batchCount, batchSize)
  const totalSizeToAggregate = batches.flat().reduce((accum, car) => accum + car.size, 0)

  const aggregateTable = createAggregateTable(REGION, aggregateTableName, {
    ferryTableName,
    endpoint: t.context.dbEndpoint,
    minSize: totalSizeToAggregate / 2,
    maxSize: totalSizeToAggregate
  })

  // Insert batches into aggregate table
  for (const batch of batches) {
    await aggregateTable.appendCARs(aggregateId, batch)
  }

  const aggregatesBeforeLock = await getAggregates(t.context.dynamoClient, aggregateTableName)
  // only one aggregate exists all the time
  t.is(aggregatesBeforeLock.length, 1)
  t.is(aggregatesBeforeLock[0].aggregateId, aggregateId)
  t.is(aggregatesBeforeLock[0].stat, AGGREGATE_STATE.ingesting)

  // attempt to set deal as pending
  await t.throwsAsync(() => aggregateTable.setAsDealPending(aggregatesBeforeLock[0].aggregateId))

  const aggregatesNotPending = await getAggregates(t.context.dynamoClient, aggregateTableName)
  // only one aggregate exists all the time
  t.is(aggregatesNotPending.length, 1)
  t.is(aggregatesNotPending[0].aggregateId, aggregateId)
  t.is(aggregatesNotPending[0].stat, AGGREGATE_STATE.ingesting)
})

test('fails to set as deal processed when aggregate is not pending', async t => {
  const batchSize = 10
  const batchCount = 1
  const aggregateId = `${Date.now()}`

  const { aggregateTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const batches = await getBatchesToAggregate(batchCount, batchSize)
  const totalSizeToAggregate = batches.flat().reduce((accum, car) => accum + car.size, 0)

  const aggregateTable = createAggregateTable(REGION, aggregateTableName, {
    ferryTableName,
    endpoint: t.context.dbEndpoint,
    minSize: totalSizeToAggregate / 2,
    maxSize: totalSizeToAggregate
  })

  // Insert batches into aggregate table
  for (const batch of batches) {
    await aggregateTable.appendCARs(aggregateId, batch)
  }

  const aggregatesBeforeLock = await getAggregates(t.context.dynamoClient, aggregateTableName)
  // only one aggregate exists all the time
  t.is(aggregatesBeforeLock.length, 1)
  t.is(aggregatesBeforeLock[0].aggregateId, aggregateId)
  t.is(aggregatesBeforeLock[0].stat, AGGREGATE_STATE.ingesting)

  // set aggregate as ready
  await aggregateTable.setAsReady(aggregateId)

  const aggregatesAfterLock = await getAggregates(t.context.dynamoClient, aggregateTableName)
  // only one aggregate exists all the time
  t.is(aggregatesAfterLock.length, 1)
  t.is(aggregatesAfterLock[0].stat, AGGREGATE_STATE.ready)

  // set aggregate as deal processed
  const commP = 'commP...a'
  // attempt to set deal as processed
  await t.throwsAsync(() => aggregateTable.setAsDealProcessed(aggregatesAfterLock[0].aggregateId, commP))

  const aggregatesNotProcessed = await getAggregates(t.context.dynamoClient, aggregateTableName)
  // only one aggregate exists all the time
  t.is(aggregatesNotProcessed.length, 1)
  t.is(aggregatesNotProcessed[0].aggregateId, aggregateId)
  t.is(aggregatesNotProcessed[0].stat, AGGREGATE_STATE.ready)
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
 * @param {import("@aws-sdk/client-dynamodb").DynamoDBClient} dynamo
 * @param {string} tableName
 * @param {object} [options]
 * @param {number} [options.limit]
 */
async function getCarsInAggregateFerry (dynamo, tableName, options = {}) {
  const cmd = new ScanCommand({
    TableName: tableName,
    Limit: options.limit || 1000
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

/**
 * @param {number} length
 * @param {number} batchSize
 */
async function getBatchesToAggregate (length, batchSize) {
  return Promise.all(
    Array.from({ length }).map(() => getCars(batchSize))
  )
}
