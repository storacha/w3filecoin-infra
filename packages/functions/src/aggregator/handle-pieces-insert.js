import * as Sentry from '@sentry/serverless'
import { unmarshall } from '@aws-sdk/util-dynamodb'

import { decodeRecord } from '@w3filecoin/core/src/store/aggregator-piece-store.js'
import { createClient as createBufferStoreClient } from '@w3filecoin/core/src/store/aggregator-buffer-store.js'
import { createClient as createBufferQueueClient } from '@w3filecoin/core/src/queue/buffer-queue.js'
import * as aggregatorEvents from '@web3-storage/filecoin-api/aggregator/events'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 0,
})

/**
 * @typedef {import('@web3-storage/filecoin-api/aggregator/api').PieceRecord} PieceRecord
 * @typedef {import('@w3filecoin/core/src/store/types').AggregatorPieceStoreRecord} AggregatorPieceStoreRecord
 */

/**
 * Get EventRecord from the SQS Event triggering the handler.
 * On Piece store insert batch, buffer pieces together to resume buffer processing.
 *
 * @param {import('aws-lambda').DynamoDBStreamEvent} event
 */
async function handlePiecesInsert (event) {
  // Get context
  const context = getContext()

  // Parse records
  const eventRawRecords = parseDynamoDbEvent(event)
  const records = eventRawRecords.map(r => {
    /** @type {AggregatorPieceStoreRecord} */
    // @ts-expect-error can't figure out type of new
    const storeRecord = unmarshall(r.new)
    return decodeRecord(storeRecord)
  })

  const { ok, error } = await aggregatorEvents.handlePiecesInsert(context, records)
  if (error) {
    return {
      statusCode: 500,
      body: error.message || 'failed to handle piece insert event'
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
    bufferQueueUrl,
    bufferQueueRegion,
  } = getEnv()

  return {
    bufferStore: createBufferStoreClient(
      { region: bufferStoreBucketRegion },
      { name: bufferStoreBucketName }
    ),
    bufferQueue: createBufferQueueClient(
      { region: bufferQueueRegion },
      { queueUrl: bufferQueueUrl }
    ),
  }
}

/**
 * Get Env validating it is set.
 */
function getEnv () {
  return {
    bufferStoreBucketName: mustGetEnv('BUFFER_STORE_BUCKET_NAME'),
    bufferStoreBucketRegion: mustGetEnv('AWS_REGION'),
    bufferQueueUrl: mustGetEnv('BUFFER_QUEUE_URL'),
    bufferQueueRegion: mustGetEnv('AWS_REGION'),
  }
}

export const main = Sentry.AWSLambda.wrapHandler(handlePiecesInsert)
