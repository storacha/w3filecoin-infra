import { test } from '../helpers/context.js'
import { createDynamodDb, dynamoDBTableConfig } from '../helpers/resources.js'
import { getCars } from '../helpers/car.js'

import { CreateTableCommand, ScanCommand } from '@aws-sdk/client-dynamodb'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { customAlphabet } from 'nanoid'

import { cargoTableProps, ferryTableProps } from '../../tables/index.js'
import { createFerryTable, FERRY_STATE } from '../../tables/ferry.js'

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

test('can add multiple batches to the same ferry', async t => {
  const batchSize = 10
  const batchCount = 2
  const ferryId = `${Date.now()}`

  const { cargoTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const batches = await getBatchesToLoad(batchCount, batchSize)
  const totalSizeToTransport = batches.flat().reduce((accum, car) => accum + car.size, 0)

  const ferryTable = createFerryTable(REGION, ferryTableName, {
    cargoTableName,
    endpoint: t.context.dbEndpoint,
    // Make max size enough for sum of all batches size
    maxSize: totalSizeToTransport
  })

  // Insert batches into ferry table
  for (const [i, batch] of batches.entries()) {
    await ferryTable.addCargo(ferryId, batch)

    const ferries = await getFerries(t.context.dynamoClient, ferryTableName)
    const carsInFerry = await getCargoInFerry(t.context.dynamoClient, cargoTableName)

    // only one ferry exists all the time
    t.is(ferries.length, 1)

    t.is(carsInFerry.length, batchSize + (i * batchSize))
    t.is(ferries[0].stat, FERRY_STATE.loading)
  }

  const ferries = await getFerries(t.context.dynamoClient, ferryTableName)
  const carsInFerry = await getCargoInFerry(t.context.dynamoClient, cargoTableName)

  // only one ferry exists all the time
  t.is(ferries.length, 1)
  t.is(carsInFerry.length, batchSize * batchCount)
  t.is(ferries[0].id, ferryId)
  t.is(ferries[0].stat, FERRY_STATE.loading)
  t.is(ferries[0].size, totalSizeToTransport)
  t.truthy(ferries[0].insertedAt)
  t.truthy(ferries[0].updatedAt)
  t.not(ferries[0].insertedAt, ferries[0].updatedAt)

  // Validate all CARs in ferry are assigned to given ferry
  for (const car of carsInFerry) {
    t.is(car.ferryId, ferryId)
  }
})

test('fails to insert a new batch if its size is bigger than max', async t => {
  const batchSize = 10
  const batchCount = 1
  const ferryId = `${Date.now()}`

  const { cargoTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const batches = await getBatchesToLoad(batchCount, batchSize)
  const totalSizeToTransport = batches.flat().reduce((accum, car) => accum + car.size, 0)

  const ferryTable = createFerryTable(REGION, ferryTableName, {
    cargoTableName,
    endpoint: t.context.dbEndpoint,
    // Make max size smaller than requested content to load on ferry
    maxSize: totalSizeToTransport - 1
  })

  await t.throwsAsync(() => ferryTable.addCargo(ferryId, batches[0]))

  const ferries = await getFerries(t.context.dynamoClient, ferryTableName)
  // no ferries exist
  t.is(ferries.length, 0)
})

test('fails to insert a second batch if total size is bigger than max', async t => {
  const batchSize = 10
  const batchCount = 2
  const ferryId = `${Date.now()}`

  const { cargoTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const batches = await getBatchesToLoad(batchCount, batchSize)
  const totalSizeToTransport = batches.flat().reduce((accum, car) => accum + car.size, 0)

  const ferryTable = createFerryTable(REGION, ferryTableName, {
    cargoTableName,
    endpoint: t.context.dbEndpoint,
    // Make max size smaller than requested content for the ferry to load
    maxSize: totalSizeToTransport - 1
  })

  // First batch succeeds to insert
  await ferryTable.addCargo(ferryId, batches[0])

  // Second batch fails to insert
  await t.throwsAsync(() => ferryTable.addCargo(ferryId, batches[1]))

  const ferries = await getFerries(t.context.dynamoClient, ferryTableName)
  const carsInFerry = await getCargoInFerry(t.context.dynamoClient, cargoTableName)
  
  t.is(ferries.length, 1)
  t.is(carsInFerry.length, Number(batchSize) * 1)
  t.is(ferries[0].stat, FERRY_STATE.loading)
  t.is(ferries[0].id, ferryId)
  t.is(ferries[0].size, batches[0].reduce((accum, car) => accum + car.size, 0))
  t.truthy(ferries[0].insertedAt)
  t.truthy(ferries[0].updatedAt)
  t.is(ferries[0].insertedAt, ferries[0].updatedAt)
})

test('can transition ferry states', async t => {
  const batchSize = 10
  const batchCount = 1
  const ferryId = `${Date.now()}`

  const { cargoTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const batches = await getBatchesToLoad(batchCount, batchSize)
  const totalSizeToTransport = batches.flat().reduce((accum, car) => accum + car.size, 0)

  const ferryTable = createFerryTable(REGION, ferryTableName, {
    cargoTableName,
    endpoint: t.context.dbEndpoint,
    minSize: totalSizeToTransport / 2,
    maxSize: totalSizeToTransport
  })

  // Insert batches into ferry table
  for (const batch of batches) {
    await ferryTable.addCargo(ferryId, batch)
  }

  const ferriesBeforeLock = await getFerries(t.context.dynamoClient, ferryTableName)
  // only one ferry exists all the time
  t.is(ferriesBeforeLock.length, 1)
  t.is(ferriesBeforeLock[0].id, ferryId)
  t.is(ferriesBeforeLock[0].stat, FERRY_STATE.loading)

  // set ferry as ready
  await ferryTable.setAsReady(ferryId)

  const ferriesAfterLock = await getFerries(t.context.dynamoClient, ferryTableName)
  // only one ferry exists all the time
  t.is(ferriesAfterLock.length, 1)
  t.is(ferriesAfterLock[0].stat, FERRY_STATE.ready)

  // set ferry as pending deal
  await ferryTable.setAsDealPending(ferriesAfterLock[0].id)

  const ferriesPendingDeal = await getFerries(t.context.dynamoClient, ferryTableName)
  // only one ferry exists all the time
  t.is(ferriesPendingDeal.length, 1)
  t.is(ferriesPendingDeal[0].id, ferryId)
  t.is(ferriesPendingDeal[0].stat, FERRY_STATE.dealPending)
  t.falsy(ferriesPendingDeal[0].commP)

  // set ferry as deal processed
  const commP = 'commP...a'
  await ferryTable.setAsDealProcessed(ferriesAfterLock[0].id, commP)

  const ferriesProcessedDeal = await getFerries(t.context.dynamoClient, ferryTableName)
  // only one ferry exists all the time
  t.is(ferriesProcessedDeal.length, 1)
  t.is(ferriesProcessedDeal[0].id, ferryId)
  t.is(ferriesProcessedDeal[0].stat, FERRY_STATE.dealProcessed)
  t.is(ferriesProcessedDeal[0].commP, commP)
})

test('fails to get ferry ready without a minimum size', async t => {
  const batchSize = 10
  const batchCount = 2
  const ferryId = `${Date.now()}`

  const { cargoTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const batches = await getBatchesToLoad(batchCount, batchSize)
  const totalSizeToTransport = batches.flat().reduce((accum, car) => accum + car.size, 0)

  const ferryTable = createFerryTable(REGION, ferryTableName, {
    cargoTableName,
    endpoint: t.context.dbEndpoint,
    // Needs more than one batch minimum
    minSize: (totalSizeToTransport / 2) + 1,
    maxSize: totalSizeToTransport
  })

  // First batch succeeds to insert
  await ferryTable.addCargo(ferryId, batches[0])

  await t.throwsAsync(() => ferryTable.setAsReady(ferryId))

  const ferriesBeforeLock = await getFerries(t.context.dynamoClient, ferryTableName)
  // only one ferry exists all the time
  t.is(ferriesBeforeLock.length, 1)
  t.is(ferriesBeforeLock[0].id, ferryId)
  t.is(ferriesBeforeLock[0].stat, FERRY_STATE.loading)

  // First batch succeeds to insert
  await ferryTable.addCargo(ferryId, batches[1])

  await ferryTable.setAsReady(ferryId)
  const ferriesAfterLock = await getFerries(t.context.dynamoClient, ferryTableName)
  // only one ferry exists all the time
  t.is(ferriesAfterLock.length, 1)
  t.is(ferriesAfterLock[0].id, ferryId)
  t.is(ferriesAfterLock[0].stat, FERRY_STATE.ready)
})

test('fails to set as deal pending when ferry not ready', async t => {
  const batchSize = 10
  const batchCount = 1
  const ferryId = `${Date.now()}`

  const { cargoTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const batches = await getBatchesToLoad(batchCount, batchSize)
  const totalSizeToTransport = batches.flat().reduce((accum, car) => accum + car.size, 0)

  const ferryTable = createFerryTable(REGION, ferryTableName, {
    cargoTableName,
    endpoint: t.context.dbEndpoint,
    minSize: totalSizeToTransport / 2,
    maxSize: totalSizeToTransport
  })

  // Insert batches into ferry table
  for (const batch of batches) {
    await ferryTable.addCargo(ferryId, batch)
  }

  const ferriesBeforeLock = await getFerries(t.context.dynamoClient, ferryTableName)
  // only one ferry exists all the time
  t.is(ferriesBeforeLock.length, 1)
  t.is(ferriesBeforeLock[0].id, ferryId)
  t.is(ferriesBeforeLock[0].stat, FERRY_STATE.loading)

  // attempt to set deal as pending
  await t.throwsAsync(() => ferryTable.setAsDealPending(ferriesBeforeLock[0].id))

  const ferriesNotPending = await getFerries(t.context.dynamoClient, ferryTableName)
  // only one ferry exists all the time
  t.is(ferriesNotPending.length, 1)
  t.is(ferriesNotPending[0].id, ferryId)
  t.is(ferriesNotPending[0].stat, FERRY_STATE.loading)
})

test('fails to set as deal processed when ferry is not pending', async t => {
  const batchSize = 10
  const batchCount = 1
  const ferryId = `${Date.now()}`

  const { cargoTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const batches = await getBatchesToLoad(batchCount, batchSize)
  const totalSizeToTransport = batches.flat().reduce((accum, car) => accum + car.size, 0)

  const ferryTable = createFerryTable(REGION, ferryTableName, {
    cargoTableName,
    endpoint: t.context.dbEndpoint,
    minSize: totalSizeToTransport / 2,
    maxSize: totalSizeToTransport
  })

  // Insert batches into ferry table
  for (const batch of batches) {
    await ferryTable.addCargo(ferryId, batch)
  }

  const ferriesBeforeLock = await getFerries(t.context.dynamoClient, ferryTableName)
  // only one ferry exists all the time
  t.is(ferriesBeforeLock.length, 1)
  t.is(ferriesBeforeLock[0].id, ferryId)
  t.is(ferriesBeforeLock[0].stat, FERRY_STATE.loading)

  // set ferry as ready
  await ferryTable.setAsReady(ferryId)

  const ferriesAfterLock = await getFerries(t.context.dynamoClient, ferryTableName)
  // only one ferry exists all the time
  t.is(ferriesAfterLock.length, 1)
  t.is(ferriesAfterLock[0].stat, FERRY_STATE.ready)

  // set ferry as deal processed
  const commP = 'commP...a'
  // attempt to set deal as processed
  await t.throwsAsync(() => ferryTable.setAsDealProcessed(ferriesAfterLock[0].id, commP))

  const ferriesNotProcessed = await getFerries(t.context.dynamoClient, ferryTableName)
  // only one ferry exists all the time
  t.is(ferriesNotProcessed.length, 1)
  t.is(ferriesNotProcessed[0].id, ferryId)
  t.is(ferriesNotProcessed[0].stat, FERRY_STATE.ready)
})

/**
 * @param {import("@aws-sdk/client-dynamodb").DynamoDBClient} dynamo
 * @param {string} tableName
 * @param {object} [options]
 * @param {number} [options.limit]
 */
async function getFerries (dynamo, tableName, options = {}) {
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
async function getCargoInFerry (dynamo, tableName, options = {}) {
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
  const [ cargoTableName, ferryTableName ] = await Promise.all([
    createDynamoCargoTable(dynamoClient),
    createDynamoFerryTable(dynamoClient),
  ])

  return {
    cargoTableName,
    ferryTableName
  }
}

/**
 * @param {import("@aws-sdk/client-dynamodb").DynamoDBClient} dynamo
 */
async function createDynamoCargoTable(dynamo) {
  const id = customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10)
  const tableName = id()

  await dynamo.send(new CreateTableCommand({
    TableName: tableName,
    ...dynamoDBTableConfig(cargoTableProps),
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
async function getBatchesToLoad (length, batchSize) {
  return Promise.all(
    Array.from({ length }).map(() => getCars(batchSize))
  )
}
