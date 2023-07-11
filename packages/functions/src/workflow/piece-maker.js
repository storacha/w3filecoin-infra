
import * as Sentry from '@sentry/serverless'
import { RDS } from 'sst/node/rds'
import { SQSClient } from '@aws-sdk/client-sqs'

import { createContentFetcher } from '@w3filecoin/core/src/content-fetcher'
import { createContentQueue } from '@w3filecoin/core/src/queue/content'
import { createPieceQueue } from '@w3filecoin/core/src/queue/piece'
import * as pieceMakerWorkflow from '@w3filecoin/core/src/workflow/piece-maker'

import { mustGetEnv, parseContentQueueEvent } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * Reads queued content and adds it to a queue
 */
async function consumerHandler() {
  const { db, queueUrl, queueRegion } = getConsumerEnv()
  const contentQueue = createContentQueue(db)
  const sqsClient = new SQSClient({
    region: queueRegion,
  })

  const { ok, error } = await pieceMakerWorkflow.consumer({ contentQueue, sqsClient, queueUrl })
  if (error) {
    return {
      statusCode: 500,
      body: error.name
    }
  }

  return {
    statusCode: 200,
    body: ok?.count
  }
}

/**
 * Get EventRecord from the SQS Event triggering the handler and 
 *
 * @param {import('aws-lambda').SQSEvent} event
 */
async function producerHandler(event) {
  const { db, contentFetcherUrlR2 } = getProducerEnv()
  const pieceQueue = createPieceQueue(db)
  const contentFetcher = createContentFetcher(
    { clientOpts: {} },
    { httpEndpoint: contentFetcherUrlR2 }
  )

  const item = parseContentQueueEvent(event)
  if (!item) {
    throw new Error('Invalid content format')
  }

  const { error } = await pieceMakerWorkflow.producer({ item, pieceQueue, contentFetcher })
  if (error) {
    // TODO: Should we handle content fetcher error differently?
    return {
      statusCode: 500,
      body: error.name
    }
  }

  return {
    statusCode: 200
  }
}

/**
 * Get Env validating it is set.
 */
function getConsumerEnv () {
  return {
    db: getDbEnv(),
    queueUrl: mustGetEnv('QUEUE_URL'),
    queueRegion: mustGetEnv('QUEUE_REGION'),
    contentFetcherUrlR2: mustGetEnv('CONTENT_FETCHER_URL_R2')
  }
}

/**
 * Get Env validating it is set.
 */
function getProducerEnv () {
  return {
    db: getDbEnv(),
    contentFetcherUrlR2: mustGetEnv('CONTENT_FETCHER_URL_R2')
  }
}

function getDbEnv () {
  const { defaultDatabaseName, secretArn, clusterArn} = RDS.Cluster

  return {
    database: defaultDatabaseName,
    secretArn,
    resourceArn: clusterArn,
  }
}

export const consumer = Sentry.AWSLambda.wrapHandler(consumerHandler)
export const producer = Sentry.AWSLambda.wrapHandler(producerHandler)
