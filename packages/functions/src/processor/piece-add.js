import * as Sentry from '@sentry/serverless'
import { Config } from 'sst/node/config'

import { getServiceConnection, getServiceSigner } from '@w3filecoin/core/src/service.js'
import { createQueueClient } from '@w3filecoin/core/src/queue/client'
import { encode } from '@w3filecoin/core/src/data/piece.js'
import { addPieces } from '@w3filecoin/core/src/workflow/piece-add'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * Get EventRecord from the SQS Event triggering the handler.
 * The event contains a batch of `piece`s provided from producer.
 * These pieces should be added using `piece/add` and propagated for piece buffering.
 *
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 */
async function pieceAddWorkflow (sqsEvent) {
  const { queueClient } = getProps()
  const { aggregatorDid, aggregatorUrl } = getEnv()
  const { PRIVATE_KEY: privateKey } = Config

  const aggregatorServiceConnection = getServiceConnection({
    did: aggregatorDid,
    url: aggregatorUrl
  })
  const issuer = getServiceSigner({
    did: aggregatorDid,
    privateKey
  })
  /** @type {import('@web3-storage/filecoin-client/types').InvocationConfig} */
  const invocationConfig = {
    issuer,
    audience: aggregatorServiceConnection.id,
    with: issuer.did(),
  }

  const records = sqsEvent.Records.map(r => ({
    body: r.body,
    id: r.messageId
  }))

  try {
    const { error } = await addPieces({
      queueClient,
      aggregatorServiceConnection,
      invocationConfig,
      records,
    })
  
    return {
      // 200 status code also applies when partial batch response
      // Read more: https://docs.aws.amazon.com/prescriptive-guidance/latest/lambda-event-filtering-partial-batch-responses-for-sqs/welcome.html
      statusCode: 200,
      body: records.length - (error?.length || 0),
      // to retry failed items from batch
      batchItemFailures: error?.map(e => e?.id)
    }
  } catch (/** @type {any} */ error) {
    return {
      statusCode: 500,
      body: error.message || 'failed to add aggregate'
    }
  }
}

/**
 * Get props clients
 */
function getProps () {
  const { pieceBufferQueueUrl, pieceBufferQueueRegion } = getEnv()

  return {
    queueClient: createQueueClient({
      region: pieceBufferQueueRegion
    }, {
      queueUrl: pieceBufferQueueUrl,
      encodeMessage: encode.message,
    })
  }
}

/**
 * Get Env validating it is set.
 */
function getEnv () {
  return {
    pieceBufferQueueUrl: mustGetEnv('PIECE_BUFFER_QUEUE_URL'),
    pieceBufferQueueRegion: mustGetEnv('PIECE_BUFFER_QUEUE_REGION'),
    aggregatorDid: mustGetEnv('AGGREGATOR_DID'),
    aggregatorUrl: mustGetEnv('AGGREGATOR_URL'),
  }
}

export const workflow = Sentry.AWSLambda.wrapHandler(pieceAddWorkflow)
