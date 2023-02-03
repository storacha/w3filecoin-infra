import { test } from './helpers/context.js'
import { createDynamodDb, dynamoDBTableConfig } from './helpers/resources.js'
import { getCars } from './helpers/car.js'

import { CreateTableCommand, ScanCommand } from '@aws-sdk/client-dynamodb'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { customAlphabet } from 'nanoid'

import { cargoTableProps, ferryTableProps } from '../tables/index.js'
import { FERRY_STATE } from '../tables/ferry.js'
import { addCarsToFerry } from '../lib/add-cars-to-ferry.js'
import { setFerryAsReady } from '../lib/set-ferry-as-ready.js'

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

test('can add cars to given ferry', async t => {
  const { ferryTableName, ferryProps } = await getTable(t)
  const cars = await getCars(10)
  const { id: ferryId } = await addCarsToFerry(cars, ferryProps)

  const ferries = await getFerries(t.context.dynamoClient, ferryTableName)
  t.is(ferries.length, 1)
  t.is(ferries[0].id, ferryId)
  t.is(ferries[0].stat, FERRY_STATE.loading)
})

test('can add cars to same ferry', async t => {
  const { ferryTableName, ferryProps } = await getTable(t)
  const batches = await Promise.all([
    getCars(10),
    getCars(10)
  ])

  const { id: ferryId0 } = await addCarsToFerry(batches[0], ferryProps)

  const ferriesAfterFirstBatch = await getFerries(t.context.dynamoClient, ferryTableName)
  t.is(ferriesAfterFirstBatch.length, 1)
  t.is(ferriesAfterFirstBatch[0].id, ferryId0)
  t.is(ferriesAfterFirstBatch[0].stat, FERRY_STATE.loading)

  const { id: ferryId1 } = await addCarsToFerry(batches[1], ferryProps)
  t.is(ferryId0, ferryId1)

  const ferriesAfterSecondBatch = await getFerries(t.context.dynamoClient, ferryTableName)
  t.is(ferriesAfterSecondBatch.length, 1)
  t.is(ferriesAfterSecondBatch[0].id, ferryId1)
  t.is(ferriesAfterSecondBatch[0].stat, FERRY_STATE.loading)
})

test('can set a ferry as ready', async t => {
  const { ferryTableName, ferryProps } = await getTable(t)
  const cars = await getCars(10)

  const { id: ferryId } = await addCarsToFerry(cars, ferryProps)
  await setFerryAsReady(ferryId, ferryProps)

  const ferries = await getFerries(t.context.dynamoClient, ferryTableName)
  t.is(ferries.length, 1)
  t.is(ferries[0].id, ferryId)
  t.is(ferries[0].stat, FERRY_STATE.ready)
})

test('can handle concurrent set of a ferry as ready', async t => {
  const { ferryTableName, ferryProps } = await getTable(t)
  const cars = await getCars(10)

  const { id: ferryId } = await addCarsToFerry(cars, ferryProps)

  // Concurrent set ferry as ready
  await Promise.all([
    setFerryAsReady(ferryId, ferryProps),
    setFerryAsReady(ferryId, ferryProps)
  ])

  const ferries = await getFerries(t.context.dynamoClient, ferryTableName)
  t.is(ferries.length, 1)
  t.is(ferries[0].id, ferryId)
  t.is(ferries[0].stat, FERRY_STATE.ready)
})

test('can handle concurrent ferries in ready state', async t => {
  let { ferryTableName, ferryProps } = await getTable(t)
  const batches = await Promise.all([
    getCars(10),
    getCars(10)
  ])

  // Simulare race condition
  // Attempt concurrent operations in new table until one exists
  let ferriesResponses
  do {
    const table = await getTable(t)
    ferryTableName = table.ferryTableName
    ferryProps = table.ferryProps

    ferriesResponses = await Promise.all(
      batches.map(batch => addCarsToFerry(batch, table.ferryProps))
    )
  } while (ferriesResponses[0].id === ferriesResponses[1].id)

  // Concurrent requests resulted in concurrent ferries ingesting
  t.not(ferriesResponses[0].id, ferriesResponses[1].id)
  const concurrentIngestingFerries = await getFerries(t.context.dynamoClient, ferryTableName)
  t.is(concurrentIngestingFerries.length, 2)
  
  for (const ferry of concurrentIngestingFerries) {
    t.is(ferry.stat, FERRY_STATE.loading)
  }

  const moreBatches = await Promise.all([
    getCars(10),
    getCars(10)
  ])

  // Adds to one of the previous ferries
  const { id: ferryId0 } = await addCarsToFerry(moreBatches[0], ferryProps)
  t.truthy(ferriesResponses.find(res => res.id === ferryId0))
  
  // Adds to other previous ferry when one finishes
  await setFerryAsReady(ferryId0, ferryProps)
  const { id: ferryId1 } = await addCarsToFerry(moreBatches[0], ferryProps)
  t.truthy(ferriesResponses.find(res => res.id === ferryId1))

  t.not(ferryId0, ferryId1)
})

/**
 * @param {import("ava").ExecutionContext<import("./helpers/context.js").FerryContext>} t
 */
async function getTable (t) {
  const { cargoTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const ferryProps = {
    region: REGION,
    tableName: ferryTableName,
    options: {
      cargoTableName,
      endpoint: t.context.dbEndpoint,
      minSize: 1
    }
  }

  return { ferryProps, ferryTableName }
}

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
