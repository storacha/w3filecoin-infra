import * as Sentry from '@sentry/serverless'

import { createClient as createBufferStoreClient } from '@w3filecoin/core/src/store/aggregator-buffer-store.js'
import { createClient as createBufferQueueClient, decodeMessage } from '@w3filecoin/core/src/queue/buffer-queue.js'
import { createClient as createAggregateOfferQueueClient } from '@w3filecoin/core/src/queue/aggregate-offer-queue.js'
import * as aggregatorEvents from '@web3-storage/filecoin-api/aggregator/events'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * On buffer queue messages, reduce received buffer records into a bigger buffer.
 * - If new buffer does not have enough load to build an aggregate, it is stored
 * and requeued for buffer reducing
 * - If new buffer has enough load to build an aggregate, it is stored and queued
 * into aggregateOfferQueue. Remaining of the new buffer (in case buffer bigger
 * than maximum aggregate size) is re-queued into the buffer queue.
 *
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 */
async function handleBufferQueueMessage (sqsEvent) {
  // if one we should put back in queue
  if (sqsEvent.Records.length === 1) {
    return {
      batchItemFailures: sqsEvent.Records.map(r => ({
        itemIdentifier: r.messageId
      }))
    }
  }

  // unexpected number of records
  if (sqsEvent.Records.length !== 2) {
    return {
      statusCode: 400,
      body: `Expected 1 sqsEvent per invocation but received ${sqsEvent.Records.length}`
    }
  }

  // Get context
  const context = getContext()
  // Parse records
  const records = sqsEvent.Records.map(r => {
    return decodeMessage({
      MessageBody: r.body
    })
  })

  const { ok, error } = await aggregatorEvents.handleBufferQueueMessage(context, records)
  if (error) {
    return {
      statusCode: 500,
      body: error.message || 'failed to handle buffer queue message'
    }
  }

  return {
    statusCode: 200,
    body: ok
  }
}

function getContext () {
  const {
    bufferStoreBucketName,
    bufferStoreBucketRegion,
    bufferQueueUrl,
    bufferQueueRegion,
    aggregateOfferQueueUrl,
    aggregateOfferQueueRegion,
    maxAggregateSize,
    minAggregateSize,
    minUtilizationFactor
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
    aggregateOfferQueue: createAggregateOfferQueueClient(
      { region: aggregateOfferQueueRegion },
      { queueUrl: aggregateOfferQueueUrl }
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
    bufferQueueUrl: mustGetEnv('BUFFER_QUEUE_URL'),
    bufferQueueRegion: mustGetEnv('AWS_REGION'),
    aggregateOfferQueueUrl: mustGetEnv('AGGREGATE_OFFER_QUEUE_URL'),
    aggregateOfferQueueRegion: mustGetEnv('AWS_REGION'),
    maxAggregateSize: Number.parseInt(mustGetEnv('MAX_AGGREGATE_SIZE')),
    minAggregateSize: Number.parseInt(mustGetEnv('MIN_AGGREGATE_SIZE')),
    minUtilizationFactor: Number.parseInt(mustGetEnv('MIN_UTILIZATION_FACTOR')),
  }
}

export const main = Sentry.AWSLambda.wrapHandler(handleBufferQueueMessage)
