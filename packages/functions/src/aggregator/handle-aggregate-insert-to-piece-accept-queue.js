import * as Sentry from '@sentry/serverless'
import { unmarshall } from '@aws-sdk/util-dynamodb'

import { decodeRecord } from '@w3filecoin/core/src/store/aggregator-aggregate-store.js'
import { createClient as createBufferStoreClient } from '@w3filecoin/core/src/store/aggregator-buffer-store.js'
import { createClient as createPieceAcceptQueueClient } from '@w3filecoin/core/src/queue/piece-accept-queue.js'
import * as aggregatorEvents from '@web3-storage/filecoin-api/aggregator/events'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * @typedef {import('@web3-storage/filecoin-api/aggregator/api').AggregateRecord} AggregateRecord
 * @typedef {import('@w3filecoin/core/src/store/types').InferStoreRecord<AggregateRecord>} InferStoreRecord
 */

/**
 * Get EventRecord from the SQS Event triggering the handler.
 * On Piece store insert batch, buffer pieces together to resume buffer processing.
 *
 * @param {import('aws-lambda').DynamoDBStreamEvent} event
 */
async function handleAggregateInsertToPieceAcceptQueue (event) {
  // Parse records
  const eventRawRecords = parseDynamoDbEvent(event)
  // if one we should put back in queue
  if (eventRawRecords.length !== 1) {
    return {
      statusCode: 400,
      body: `Expected 1 DynamoDBStreamEvent per invocation but received ${eventRawRecords.length}`
    }
  }
  /** @type {InferStoreRecord} */
  // @ts-expect-error can't figure out type of new
  const storeReecord = unmarshall(eventRawRecords[0].new)
  const record = decodeRecord(storeReecord)

  // Get context
  const context = getContext()

  const { ok, error } = await aggregatorEvents.handleAggregateInsertToPieceAcceptQueue(context, record)
  if (error) {
    return {
      statusCode: 500,
      body: error.message || 'failed to handle aggregate insert event'
    }
  }

  return {
    statusCode: 200,
    body: ok
  }
}

/**
 * @param {import('aws-lambda').DynamoDBStreamEvent} event
 */
function parseDynamoDbEvent (event) {
  return event.Records.map(r => ({
    new: r.dynamodb?.NewImage,
    old: r.dynamodb?.OldImage
  }))
}

function getContext () {
  const {
    bufferStoreBucketName,
    bufferStoreBucketRegion,
    pieceAcceptQueueUrl,
    pieceAcceptQueueRegion,
    maxAggregateSize,
      minAggregateSize,
      minUtilizationFactor
  } = getEnv()

  return {
    bufferStore: createBufferStoreClient(
      { region: bufferStoreBucketRegion },
      { name: bufferStoreBucketName }
    ),
    pieceAcceptQueue: createPieceAcceptQueueClient(
      { region: pieceAcceptQueueRegion },
      { queueUrl: pieceAcceptQueueUrl }
    ),
    config: {
      maxAggregateSize,
      minAggregateSize,
      minUtilizationFactor
    }
  }
}

/**
 * Get Env validating it is set.
 */
function getEnv () {
  return {
    bufferStoreBucketName: mustGetEnv('BUFFER_STORE_BUCKET_NAME'),
    bufferStoreBucketRegion: mustGetEnv('AWS_REGION'),
    pieceAcceptQueueUrl: mustGetEnv('PIECE_ACCEPT_QUEUE_URL'),
    pieceAcceptQueueRegion: mustGetEnv('AWS_REGION'),
    maxAggregateSize: Number.parseInt(mustGetEnv('MAX_AGGREGATE_SIZE')),
    maxAggregatePieces: process.env.MAX_AGGREGATE_PIECES
      ? Number.parseInt(process.env.MAX_AGGREGATE_PIECES)
      : undefined,
    minAggregateSize: Number.parseInt(mustGetEnv('MIN_AGGREGATE_SIZE')),
    minUtilizationFactor: Number.parseInt(mustGetEnv('MIN_UTILIZATION_FACTOR')),
  }
}

export const main = Sentry.AWSLambda.wrapHandler(handleAggregateInsertToPieceAcceptQueue)
