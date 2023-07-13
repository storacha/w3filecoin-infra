
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
async function consumeHandler() {
  const { db, queueUrl, queueRegion } = getConsumerEnv()
  const contentQueue = createContentQueue(db)
  const sqsClient = new SQSClient({
    region: queueRegion,
  })

  const { ok, error } = await pieceMakerWorkflow.consume({ contentQueue, sqsClient, queueUrl })
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
 * Get EventRecord from the SQS Event triggering the handler, builds piece and
 * puts result into piece queue.
 *
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 */
async function buildPieceHandler(sqsEvent) {
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

  const { error } = await pieceMakerWorkflow.buildPiece({ item, pieceQueue, contentResolver })
  if (error) {
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
  const { defaultDatabaseName, secretArn, clusterArn} = RDS.w3filecoinrds

  return {
    database: defaultDatabaseName,
    secretArn,
    resourceArn: clusterArn,
  }
}

export const consume = Sentry.AWSLambda.wrapHandler(consumeHandler)
export const build = Sentry.AWSLambda.wrapHandler(buildPieceHandler)
