import { fetch } from '@web-std/fetch'
import { testData as test } from './helpers/context.js'

import { BatchWriteItemCommand, ScanCommand } from '@aws-sdk/client-dynamodb'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'
import pWaitFor from 'p-wait-for'

import { AGGREGATE_KEY } from '../stacks/config.js'
import { createCarTable } from '../data/tables/car.js'
import { AGGREGATE_STAT } from '../data/tables/aggregate.js'

import { getCars } from '../data/test/helpers/car.js'
import {
  getRedisApiEndpoint,
  getAwsRegion,
  getDynamoDb,
} from './helpers/deployment.js'

test.before(async t => {
  const region = getAwsRegion()
  const carDynamo = getDynamoDb('car')
  const aggregateDynamo = getDynamoDb('aggregate')

  t.context = {
    aggregateId: '',
    region,
    carDynamo,
    aggregateDynamo
  }
})

test.beforeEach(async t => {
  t.context.aggregateId = await getAggregateId()
})

test.afterEach(async t => {
  const { carDynamo, aggregateDynamo } = t.context

  // Delete Car Table
  await deleteCarTableRows(carDynamo.client, carDynamo.tableName, 
    await getTableRows(carDynamo.client, carDynamo.tableName)
  )

  // Delete Aggregate Table
  await deleteAggregateTableRows(aggregateDynamo.client, aggregateDynamo.tableName, 
    await getTableRows(aggregateDynamo.client, aggregateDynamo.tableName)
  )
})

test('can write in car table and gets propagated into aggregate when batch size ready', async t => {
  const { aggregateId, aggregateDynamo, carDynamo, region } = t.context
  const batchCount = 2
  const batchSize = 40

  const carTableClient = createCarTable(region, carDynamo.tableName, { endpoint: carDynamo.endpoint })
  const batches = await getBatchesToWrite(batchCount, batchSize)
  const totalSizeToAggregate = batches.flat().reduce((accum, car) => accum + car.size, 0)

  // No aggregates
  const aggregateItemsBeforeWrites = await getTableRows(aggregateDynamo.client, aggregateDynamo.tableName)
  t.is(aggregateItemsBeforeWrites.length, 0)

  // First batch succeeds to insert
  await carTableClient.batchWrite(batches[0])
  const carItemsAfterFirstBatch = await getTableRows(carDynamo.client, carDynamo.tableName)
  t.is(carItemsAfterFirstBatch.length, batchSize)

  // No aggregates while a batch not ready
  const aggregateItemsAfterFirstWrite = await getTableRows(aggregateDynamo.client, aggregateDynamo.tableName)
  t.is(aggregateItemsAfterFirstWrite.length, 0)

  // Second batch succeeds (already more than needed batch to add to aggregate)
  await carTableClient.batchWrite(batches[1])
  const carItemsAfterSecondBatch = await getTableRows(carDynamo.client, carDynamo.tableName)
  t.is(carItemsAfterSecondBatch.length, batchSize * 2)

  // Await for events to be triggered from car table and get written into aggregate table
  await pWaitFor(async () => {
    const aggrs = await getTableRows(aggregateDynamo.client, aggregateDynamo.tableName)

    // Wait until all CARs are added to aggregates
    const aggregatesTotalSize = aggrs.reduce((acc, agg) => acc + agg.size, 0)
    return Boolean(aggrs.length) && aggregatesTotalSize === totalSizeToAggregate
  }, {
    interval: 100
  })

  const aggregatesAfterWrite = await getTableRows(aggregateDynamo.client, aggregateDynamo.tableName)
  t.truthy(aggregatesAfterWrite.length >= 1)
  t.is(aggregatesAfterWrite[0].aggregateId, aggregateId)
  t.is(aggregatesAfterWrite[0].stat, AGGREGATE_STAT.ingesting)
  t.truthy(aggregatesAfterWrite[0].insertedAt)
  t.truthy(aggregatesAfterWrite[0].updatedAt)
  // Might go to other aggregates depending on events
  t.is(aggregatesAfterWrite.reduce((acc, agg) => acc + agg.size, 0), totalSizeToAggregate)
  t.is(aggregatesAfterWrite.reduce((acc, agg) => acc + agg.cars.size, 0), batchSize * batchCount)
})

test('can write in car table until an aggregate gets in ready state', async t => {
  const { aggregateId, aggregateDynamo, carDynamo, region } = t.context
  const batchCount = 4
  const batchSize = 40

  const carTableClient = createCarTable(region, carDynamo.tableName, { endpoint: carDynamo.endpoint })
  const batches = await getBatchesToWrite(batchCount, batchSize)
  const totalSizeToAggregate = batches.flat().reduce((accum, car) => accum + car.size, 0)

  // No aggregates
  const aggregateItemsBeforeWrites = await getTableRows(aggregateDynamo.client, aggregateDynamo.tableName)
  t.is(aggregateItemsBeforeWrites.length, 0)

  // Insert batches into car table
  for (const batch of batches) {
    await carTableClient.batchWrite(batch)
  }

  // Await for events to be triggered from car table and get written into aggregate table
  await pWaitFor(async () => {
    const aggrs = await getTableRows(aggregateDynamo.client, aggregateDynamo.tableName)

    // Wait until all CARs are added to aggregates
    const aggregatesTotalSize = aggrs.reduce((acc, agg) => acc + agg.size, 0)
    return Boolean(aggrs.length) && aggregatesTotalSize === totalSizeToAggregate
  }, {
    interval: 100
  })

  const aggregatesAfterWrite = await getTableRows(aggregateDynamo.client, aggregateDynamo.tableName)
  t.truthy(aggregatesAfterWrite.length)
  // First aggregate to write should have stat ready if more than one already exist
  aggregatesAfterWrite.length > 2 && t.is(aggregatesAfterWrite.find(agg => agg.aggregateId === aggregateId)?.stat, AGGREGATE_STAT.ready)
  // Must have all CARs and Size expected
  t.is(aggregatesAfterWrite.reduce((acc, agg) => acc + agg.size, 0), totalSizeToAggregate)
  t.is(aggregatesAfterWrite.reduce((acc, agg) => acc + agg.cars.size, 0), batchSize * batchCount)

  const latestAggregateId = await getAggregateId()
  if (aggregatesAfterWrite.length > 2) {
    // Updated to different aggregateId
    t.not(aggregateId, latestAggregateId)
  }

  // Depending on how events happen, we might have one of two:
  // - 2 aggregates READY + latest aggregate ID already updated for the next aggregate
  // - 1 aggregate READY + 1 aggregate INGESTING
  const currentAggregate = aggregatesAfterWrite.find(agg => agg.aggregateId === latestAggregateId)
  if (currentAggregate) {
    t.is(currentAggregate.stat, AGGREGATE_STAT.ingesting)
  }
})

/**
 * @param {import('@aws-sdk/client-dynamodb').DynamoDBClient} dynamo
 * @param {string} tableName
 * @param {object} [options]
 * @param {number} [options.limit]
 */
async function getTableRows (dynamo, tableName, options = {}) {
  const cmd = new ScanCommand({
    TableName: tableName,
    Limit: options.limit || 1000
  })

  const response = await dynamo.send(cmd)
  return response.Items?.map(i => unmarshall(i)) || []
}

/**
 * @param {import('@aws-sdk/client-dynamodb').DynamoDBClient} dynamo
 * @param {string} tableName
 * @param {Record<string, any>[]} rows
 */
async function deleteCarTableRows (dynamo, tableName, rows) {
  const deleteRows = [...rows]

  while (deleteRows.length) {
    const requests = deleteRows.splice(0, 25).map(row => ({
      DeleteRequest: {
        Key: marshall({ link: row.link })
      }
    }))
    const cmd = new BatchWriteItemCommand({
      RequestItems: {
        [tableName]: requests
      }
    })
  
    await dynamo.send(cmd)
  }
}

/**
 * @param {import('@aws-sdk/client-dynamodb').DynamoDBClient} dynamo
 * @param {string} tableName
 * @param {Record<string, any>[]} rows
 */
async function deleteAggregateTableRows (dynamo, tableName, rows) {
  const deleteRows = [...rows]

  while (deleteRows.length) {
    const requests = deleteRows.splice(0, 25).map(row => ({
      DeleteRequest: {
        Key: marshall({ aggregateId: row.aggregateId })
      }
    }))
    const cmd = new BatchWriteItemCommand({
      RequestItems: {
        [tableName]: requests
      }
    })
  
    await dynamo.send(cmd)
  }
}

/**
 * @param {number} length
 * @param {number} batchSize
 */
async function getBatchesToWrite (length, batchSize) {
  return Promise.all(
    Array.from({ length }).map(async () => {
      const cars = await (getCars(batchSize))

      return cars.map(car => ({
        link: car.link.toString(),
        size: car.size,
        commP: 'commP',
        url: 'url',
        md5: 'md5',
      }))
    })
  )
}

async function getAggregateId () {
  const request = await fetch(getRedisApiEndpoint())
  const response = await request.json()
  return response[AGGREGATE_KEY]
}
