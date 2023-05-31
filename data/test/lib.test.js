import { test } from './helpers/context.js'
import { createDynamodDb, dynamoDBTableConfig } from './helpers/resources.js'
import { getCars } from './helpers/car.js'
import { getAggregateServiceCtx, getAggregateServiceServer } from './helpers/ucanto.js'

import { CreateTableCommand, QueryCommand, ScanCommand } from '@aws-sdk/client-dynamodb'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { customAlphabet } from 'nanoid'
import pDefer from 'p-defer'

import { carTableProps, cargoTableProps, ferryTableProps } from '../tables/index.js'
import { createCarTable } from '../tables/car.js'
import { FERRY_STATE } from '../tables/ferry.js'
import {
  addCarsToFerry,
  setFerryAsReady,
  setFerryOffer,
  setFerryAsProcessed
} from '../lib/index.js'

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
  const { ferryTableName, ferryCtx, cargoTableName } = await getTable(t)
  const cars = await getCars(10)
  const { id: ferryId } = await addCarsToFerry(cars, ferryCtx)

  const ferries = await getFerries(t.context.dynamoClient, ferryTableName)
  t.is(ferries.length, 1)
  t.is(ferries[0].id, ferryId)
  t.is(ferries[0].stat, FERRY_STATE.loading)

  // Validate cargo
  const cargoInFerry = await getCargo(t.context.dynamoClient, cargoTableName, ferryId)
  t.is(cargoInFerry.length, cars.length)
  for (const cargo of cargoInFerry) {
    t.truthy(cars.find((car) => car.link.toString() === cargo.link))
  }
})

test('can add cars to same ferry', async t => {
  const { ferryTableName, ferryCtx } = await getTable(t)
  const batches = await Promise.all([
    getCars(10),
    getCars(10)
  ])

  const { id: ferryId0 } = await addCarsToFerry(batches[0], ferryCtx)

  const ferriesAfterFirstBatch = await getFerries(t.context.dynamoClient, ferryTableName)
  t.is(ferriesAfterFirstBatch.length, 1)
  t.is(ferriesAfterFirstBatch[0].id, ferryId0)
  t.is(ferriesAfterFirstBatch[0].stat, FERRY_STATE.loading)

  const { id: ferryId1 } = await addCarsToFerry(batches[1], ferryCtx)
  t.is(ferryId0, ferryId1)

  const ferriesAfterSecondBatch = await getFerries(t.context.dynamoClient, ferryTableName)
  t.is(ferriesAfterSecondBatch.length, 1)
  t.is(ferriesAfterSecondBatch[0].id, ferryId1)
  t.is(ferriesAfterSecondBatch[0].stat, FERRY_STATE.loading)
})

test('can set a ferry as ready', async t => {
  const { ferryTableName, ferryCtx } = await getTable(t)
  const cars = await getCars(10)

  const { id: ferryId } = await addCarsToFerry(cars, ferryCtx)
  await setFerryAsReady(ferryId, ferryCtx)

  const ferries = await getFerries(t.context.dynamoClient, ferryTableName)
  t.is(ferries.length, 1)
  t.is(ferries[0].id, ferryId)
  t.is(ferries[0].stat, FERRY_STATE.ready)
})

test('can handle concurrent set of a ferry as ready', async t => {
  const { ferryTableName, ferryCtx } = await getTable(t)
  const cars = await getCars(10)

  const { id: ferryId } = await addCarsToFerry(cars, ferryCtx)

  // Concurrent set ferry as ready
  await Promise.all([
    setFerryAsReady(ferryId, ferryCtx),
    setFerryAsReady(ferryId, ferryCtx)
  ])

  const ferries = await getFerries(t.context.dynamoClient, ferryTableName)
  t.is(ferries.length, 1)
  t.is(ferries[0].id, ferryId)
  t.is(ferries[0].stat, FERRY_STATE.ready)
})

test('can handle concurrent ferries in ready state', async t => {
  let { ferryTableName, ferryCtx } = await getTable(t)
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
    ferryCtx = table.ferryCtx

    ferriesResponses = await Promise.all(
      batches.map(batch => addCarsToFerry(batch, table.ferryCtx))
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
  const { id: ferryId0 } = await addCarsToFerry(moreBatches[0], ferryCtx)
  t.truthy(ferriesResponses.find(res => res.id === ferryId0))
  
  // Adds to other previous ferry when one finishes
  await setFerryAsReady(ferryId0, ferryCtx)
  const { id: ferryId1 } = await addCarsToFerry(moreBatches[0], ferryCtx)
  t.truthy(ferriesResponses.find(res => res.id === ferryId1))

  t.not(ferryId0, ferryId1)
})

test('can set a ferry offer', async t => {
  const { carCtx, ferryTableName, ferryCtx } = await getTable(t)
  const { storefront, aggregationService } = await getAggregateServiceCtx()
  // Create Ucanto service
  const aggregateOfferCall = pDefer()
  const serviceServer = await getAggregateServiceServer(aggregationService.raw, {
    onCall: (invCap) => {
      aggregateOfferCall.resolve(invCap)
    }
  })

  const cars = (await getCars(10)).map(car => ({
    // Inflate size for testing within range
    ...car,
    size: car.size * 10e6,
  }))
  await consumeCars(cars, carCtx)

  // Flow: Load cargo, ferry ready, ferry cargo offer
  const { id: ferryId } = await addCarsToFerry(cars, ferryCtx)
  await setFerryAsReady(ferryId, ferryCtx)
  await setFerryOffer(ferryId, {
    car: carCtx,
    ferry: ferryCtx,
    storefront,
    aggregationServiceConnection: serviceServer.connection
  })

  // Validate ucanto server call
  t.is(serviceServer.service.aggregate.offer.callCount, 1)
  const invCap = await aggregateOfferCall.promise
  t.is(invCap.can, 'aggregate/offer')
  t.is(invCap.nb.size, cars.reduce((accum, offer) => accum + offer.size, 0))
  // TODO: Validate offer CID

  const ferries = await getFerries(t.context.dynamoClient, ferryTableName)
  t.is(ferries.length, 1)
  t.is(ferries[0].id, ferryId)
  t.is(ferries[0].stat, FERRY_STATE.dealPending)
})

test('can set ferry as processed', async t => {
  const { carCtx, ferryTableName, ferryCtx } = await getTable(t)
  const { storefront, aggregationService } = await getAggregateServiceCtx()
  const serviceServer = await getAggregateServiceServer(aggregationService.raw)

  const cars = (await getCars(10)).map(car => ({
    // Inflate size for testing within range
    ...car,
    size: car.size * 10e6,
  }))
  await consumeCars(cars, carCtx)

  // Flow: Load cargo, ferry ready, ferry cargo offer, offer processed
  const { id: ferryId } = await addCarsToFerry(cars, ferryCtx)
  await setFerryAsReady(ferryId, ferryCtx)
  await setFerryOffer(ferryId, {
    car: carCtx,
    ferry: ferryCtx,
    storefront,
    aggregationServiceConnection: serviceServer.connection,
  })
  await setFerryAsProcessed(ferryId, 'commP', ferryCtx)

  const ferries = await getFerries(t.context.dynamoClient, ferryTableName)
  t.is(ferries.length, 1)
  t.is(ferries[0].id, ferryId)
  t.is(ferries[0].stat, FERRY_STATE.dealProcessed)
})

/**
 * @param {import("ava").ExecutionContext<import("./helpers/context.js").FerryContext>} t
 */
async function getTable (t) {
  const { carTableName, cargoTableName, ferryTableName } = await prepareResources(t.context.dynamoClient)
  const ferryCtx = {
    region: REGION,
    tableName: ferryTableName,
    options: {
      cargoTableName,
      endpoint: t.context.dbEndpoint,
      minSize: 1
    }
  }

  const carCtx = {
    region: REGION,
    tableName: carTableName,
    options: {
      endpoint: t.context.dbEndpoint,
    }
  }

  return { ferryCtx, carCtx, ferryTableName, cargoTableName }
}

/**
 * @param {import("../types.js").CarItem[]} cars
 * @param {{ region: any; tableName: any; options: any; }} carCtx
 */
async function consumeCars (cars, carCtx) {
  const carTable = createCarTable(carCtx.region, carCtx.tableName, carCtx.options)

  await carTable.batchWrite(cars)
}

/**
 * @param {import("@aws-sdk/client-dynamodb").DynamoDBClient} dynamo
 * @param {string} tableName
 * @param {string} id
 * @param {object} [options]
 * @param {number} [options.limit]
 */
async function getCargo (dynamo, tableName, id, options = {}) {
  const cmd = new QueryCommand({
    TableName: tableName,
    Limit: options.limit || 30,
    ExpressionAttributeValues: {
      ':id': { S: id },
    },
    KeyConditionExpression: 'ferryId = :id'
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
  const [ carTableName, cargoTableName, ferryTableName ] = await Promise.all([
    createDynamoTable(dynamoClient, dynamoDBTableConfig(carTableProps)),
    createDynamoTable(dynamoClient, dynamoDBTableConfig(cargoTableProps)),
    createDynamoTable(dynamoClient, dynamoDBTableConfig(ferryTableProps)),
  ])

  return {
    carTableName,
    cargoTableName,
    ferryTableName
  }
}

/**
 * @typedef {import('@aws-sdk/client-dynamodb').CreateTableCommandInput} CreateTableCommandInput
 * 
 * @param {import("@aws-sdk/client-dynamodb").DynamoDBClient} dynamo
 * @param {Pick<CreateTableCommandInput, 'AttributeDefinitions' | 'KeySchema' | 'GlobalSecondaryIndexes'>} tableProps
 */
async function createDynamoTable(dynamo, tableProps) {
  const id = customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10)
  const tableName = id()

  await dynamo.send(new CreateTableCommand({
    TableName: tableName,
    ...tableProps,
    ProvisionedThroughput: {
      ReadCapacityUnits: 1,
      WriteCapacityUnits: 1
    }
  }))

  return tableName
}
