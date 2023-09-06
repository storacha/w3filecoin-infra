import * as Sentry from '@sentry/serverless'

import { createQueueClient } from '@w3filecoin/core/src/queue/client'
import { createBucketStoreClient } from '@w3filecoin/core/src/store/bucket-client.js'
import { encode, decode } from '@w3filecoin/core/src/data/buffer.js'
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
  const records = sqsEvent.Records.map(r => ({
    body: r.body,
    id: r.messageId
  }))

  const { ok, error } = await bufferPieces({
    storeClient,
    queueClient,
    records
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
  const { bufferStoreBucketName, bufferStoreBucketRegion, bufferQueueUrl, bufferQueueRegion } = getEnv()

  return {
    storeClient: createBucketStoreClient({
      region: bufferStoreBucketRegion
    }, {
      name: bufferStoreBucketName,
      encodeRecord: encode.storeRecord,
      decodeRecord: decode.storeRecord
    }),
    queueClient: createQueueClient({
      region: bufferQueueRegion
    }, {
      queueUrl: bufferQueueUrl,
      encodeMessage: encode.message,
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
  }
}

export const workflow = Sentry.AWSLambda.wrapHandler(pieceBufferringWorkflow)
