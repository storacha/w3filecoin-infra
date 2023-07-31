import * as Sentry from '@sentry/serverless'
import { Bucket } from 'sst/node/bucket'

import { createQueueClient } from '@w3filecoin/core/src/queue/client'
import { createBucketStoreClient } from '@w3filecoin/core/src/store/client/bucket.js'
import { bufferPieces } from '@w3filecoin/core/src/workflow/piece-buffering'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * Get EventRecord from the SQS Event triggering the handler.
 * The event contains a batch of `piece`s provided from producer. These pieces should be buffered
 * so that a PieceBuffer can be created.
 * A piece buffer is stored and its references is pushed into buffer-queue.
 *
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 */
async function pieceBufferringWorkflow (sqsEvent) {
  const { storeClient, queueClient } = getProps()
  const pieceRecords = sqsEvent.Records.map(r => r.body)
  const groupId = sqsEvent.Records[0].attributes.MessageGroupId

  await bufferPieces({
    storeClient,
    queueClient,
    pieceRecords,
    groupId
  })

  return {
    statusCode: 200,
    body: pieceRecords.length
  }
}

/**
 * Get props clients
 */
function getProps () {
  const { bufferStoreBucketName, bufferStoreBucketRegion, bufferQueueUrl, bufferQueueRegion } = getEnv()

  return {
    storeClient: createBucketStoreClient({
      name: bufferStoreBucketName.bucketName,
      region: bufferStoreBucketRegion
    }),
    queueClient: createQueueClient({
      url: bufferQueueUrl,
      region: bufferQueueRegion
    })
  }
}

/**
 * Get Env validating it is set.
 */
function getEnv () {
  return {
    bufferStoreBucketName: Bucket['buffer-store'],
    bufferStoreBucketRegion: mustGetEnv('AWS_REGION'),
    bufferQueueUrl: mustGetEnv('BUFFER_QUEUE_URL'),
    bufferQueueRegion: mustGetEnv('BUFFER_QUEUE_REGION'),
  }
}

export const workflow = Sentry.AWSLambda.wrapHandler(pieceBufferringWorkflow)
