
import * as Sentry from '@sentry/serverless'
import { RDS } from 'sst/node/rds'
import { SQSClient } from '@aws-sdk/client-sqs'

import { createContentResolver } from '@w3filecoin/core/src/content-resolver'
import { createContentQueue } from '@w3filecoin/core/src/queue/content'
import { createPieceQueue } from '@w3filecoin/core/src/queue/piece'
import * as pieceMakerWorkflow from '@w3filecoin/core/src/workflow/piece-maker'

import { mustGetEnv } from '../utils.js'

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
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 */
async function producerHandler(sqsEvent) {
  if (sqsEvent.Records.length !== 1) {
    return {
      statusCode: 400,
      body: `Expected 1 sqsEvent per invocation but received ${sqsEvent.Records.length}`
    }
  }

  const { db, contentResolverUrlR2 } = getProducerEnv()
  const pieceQueue = createPieceQueue(db)
  const contentResolver = createContentResolver(
    { clientOpts: {} },
    { httpEndpoint: contentResolverUrlR2 }
  )

  const item = sqsEvent.Records[0].body

  const { error } = await pieceMakerWorkflow.producer({ item, pieceQueue, contentResolver })
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
    contentResolverUrlR2: mustGetEnv('CONTENT_RESOLVER_URL_R2')
  }
}

/**
 * Get Env validating it is set.
 */
function getProducerEnv () {
  return {
    db: getDbEnv(),
    contentResolverUrlR2: mustGetEnv('CONTENT_RESOLVER_URL_R2')
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
