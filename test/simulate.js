import { fetch } from '@web-std/fetch'
import { BatchWriteItemCommand, ScanCommand } from '@aws-sdk/client-dynamodb'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'
import pWaitFor from 'p-wait-for'

import { AGGREGATE_KEY } from '../stacks/config.js'
import { createCarTable } from '../data/tables/car.js'

import { getCars } from '../data/test/helpers/car.js'
import {
  getRedisApiEndpoint,
  getAwsRegion,
  getDynamoDb,
} from './helpers/deployment.js'

// ---- MAIN
const region = getAwsRegion()
const carDynamo = getDynamoDb('car')
const aggregateDynamo = getDynamoDb('aggregate')

// DELETE Environment
await clearEnv(carDynamo, aggregateDynamo)

// Write Environment
const aggregateId = await getAggregateId()

await writeCars(aggregateId, aggregateDynamo, carDynamo, region)
// ---- MAIN

/**
 * @param {string} aggregateId
 * @param {{ client: any; tableName: any; region?: string; endpoint?: string; }} aggregateDynamo
 * @param {{ client?: import("@aws-sdk/client-dynamodb").DynamoDBClient; tableName: any; region?: string; endpoint: any; }} carDynamo
 * @param {string} region
 */
async function writeCars (aggregateId, aggregateDynamo, carDynamo, region) {
  console.log(`start writing to aggregate ${aggregateId}`)
  const batchCount = 4
  const batchSize = 35

  const carTableClient = createCarTable(region, carDynamo.tableName, { endpoint: carDynamo.endpoint })
  const batches = await getBatchesToWrite(batchCount, batchSize)
  const totalSizeToAggregate = batches.flat().reduce((accum, car) => accum + car.size, 0)

  // Insert batches into car table
  for (const batch of batches) {
    console.log(`Write Batch of ${batchSize} CAR files with total size ${batch.reduce((accum, car) => accum + car.size, 0)}`)
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

  console.log(`total aggregates: ${aggregatesAfterWrite.length}`)
  console.log(`total CARs in aggregates: ${aggregatesAfterWrite.reduce((acc, agg) => acc + agg.cars.size, 0)}`)
  console.log(`total size in aggregates: ${aggregatesAfterWrite.reduce((acc, agg) => acc + agg.size, 0)}`)

  const latestAggregateId = await getAggregateId()
  console.log(`ready to write into aggregate ${latestAggregateId}`)
}

/**
 * @param {{ client: any; tableName: any; region?: string; endpoint?: string; }} carDynamo
 * @param {{ client: any; tableName: any; region?: string; endpoint?: string; }} aggregateDynamo
 */
async function clearEnv (carDynamo, aggregateDynamo) {
  // Delete Car Table
  await deleteCarTableRows(carDynamo.client, carDynamo.tableName, 
    await getTableRows(carDynamo.client, carDynamo.tableName)
  )

  // Delete Aggregate Table
  await deleteAggregateTableRows(aggregateDynamo.client, aggregateDynamo.tableName, 
    await getTableRows(aggregateDynamo.client, aggregateDynamo.tableName)
  )
}

async function getAggregateId () {
  const request = await fetch(getRedisApiEndpoint())
  const response = await request.json()
  return response[AGGREGATE_KEY]
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
