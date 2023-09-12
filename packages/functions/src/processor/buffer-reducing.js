import * as Sentry from '@sentry/serverless'

import { createQueueClient } from '@w3filecoin/core/src/queue/client'
import { createBucketStoreClient } from '@w3filecoin/core/src/store/bucket-client.js'
import { encode as bufferEncode, decode as bufferDecode } from '@w3filecoin/core/src/data/buffer.js'
import { encode as aggregateEncode } from '@w3filecoin/core/src/data/aggregate.js'
import { reduceBuffer } from '@w3filecoin/core/src/workflow/buffer-reducing'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * Get EventRecord from the SQS Event triggering the handler.
 * The event contains a batch of `buffers`(s) provided from `piece-queue` or `buffer-queue`.
 * 1. These buffers should be reduced into a single buffer with more pieces.
 * 2. An aggregate is built with the buffer items (properly sorted) in order to try to reach
 * a desired size for `aggregate/add`.
 * 2. 1. If possible, the new buffer is stored and its reference is sent to the `aggregate-queue`. 
 * A new buffer is created with the remaining pieces. Finally, it is stored and added to `buffer-queue`.
 * 2. 2. Otherwise, the new buffer is stored and added to the `buffer-queue`.
 *
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 */
async function bufferReducingWorkflow (sqsEvent) {
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

  const { storeClient, bufferQueueClient, aggregateQueueClient } = getProps()
  const { maxAggregateSize, minAggregateSize } = getEnv()

  const bufferRecords = sqsEvent.Records.map(r => r.body)
  const groupId = sqsEvent.Records[0].attributes.MessageGroupId
  // TODO: confirm group ID uniqueness

  const { ok, error } = await reduceBuffer({
    storeClient,
    bufferQueueClient,
    aggregateQueueClient,
    bufferRecords,
    maxAggregateSize,
    minAggregateSize,
    groupId
  })

  if (error) {
    return {
      statusCode: 500,
      body: error.message
    }
  }

  return {
    statusCode: 200,
    body: ok
  }
}

/**
 * Get props clients
 */
function getProps () {
  const { bufferStoreBucketName, bufferStoreBucketRegion, bufferQueueUrl, bufferQueueRegion, aggregateQueueUrl, aggregateQueueRegion } = getEnv()

  return {
    storeClient: createBucketStoreClient({
      region: bufferStoreBucketRegion
    }, {
      name: bufferStoreBucketName,
      encodeRecord: bufferEncode.storeRecord,
      decodeRecord: bufferDecode.storeRecord,
    }),
    bufferQueueClient: createQueueClient({
      region: bufferQueueRegion
    }, {
      queueUrl: bufferQueueUrl,
      encodeMessage: bufferEncode.message,
    }),
    aggregateQueueClient: createQueueClient({
      region: aggregateQueueRegion
    }, {
      queueUrl: aggregateQueueUrl,
      encodeMessage: aggregateEncode.message,
    })
  }
}

/**
 * Get Env validating it is set.
 */
function getEnv () {
  return {
    bufferStoreBucketName: mustGetEnv('BUFFER_STORE_BUCKET_NAME'),
    bufferStoreBucketRegion: mustGetEnv('BUFFER_STORE_REGION'),
    bufferQueueUrl: mustGetEnv('BUFFER_QUEUE_URL'),
    bufferQueueRegion: mustGetEnv('BUFFER_QUEUE_REGION'),
    aggregateQueueUrl: mustGetEnv('AGGREGATE_QUEUE_URL'),
    aggregateQueueRegion: mustGetEnv('AGGREGATE_QUEUE_REGION'),
    maxAggregateSize: Number.parseInt(mustGetEnv('MAX_AGGREGATE_SIZE')),
    minAggregateSize: Number.parseInt(mustGetEnv('MIN_AGGREGATE_SIZE')),
  }
}

export const workflow = Sentry.AWSLambda.wrapHandler(bufferReducingWorkflow)
