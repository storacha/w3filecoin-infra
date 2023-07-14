import * as Sentry from '@sentry/serverless'
import { SQSClient } from '@aws-sdk/client-sqs'
import { Config } from 'sst/node/config'

import { createView } from '@w3filecoin/core/src/database/views.js'
import { createAggregateQueue } from '@w3filecoin/core/src/queue/aggregate'
import { createDealQueue } from '@w3filecoin/core/src/queue/deal'
import * as submissionWorkflow from '@w3filecoin/core/src/workflow/submission'

import { getDbEnv, mustGetEnv, getAggregationServiceConnection } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * Reads queued content and adds it to a queue
 */
async function consumeHandler() {
  const { db, queueUrl, queueRegion } = getConsumeEnv()
  const aggregateQueue = createAggregateQueue(db)
  const sqsClient = new SQSClient({
    region: queueRegion,
  })

  const { ok, error } = await submissionWorkflow.consume({ aggregateQueue, sqsClient, queueUrl })
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
 * Get EventRecord from the SQS Event triggering the handler, builds offer and
 * puts result into deal queue.
 *
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 */
async function buildOfferHandler(sqsEvent) {
  if (sqsEvent.Records.length !== 1) {
    return {
      statusCode: 400,
      body: `Expected 1 sqsEvent per invocation but received ${sqsEvent.Records.length}`
    }
  }

  const {
    db,
    did,
    aggregationServiceDid,
    aggregationServiceUrl
  } = getBuildOfferEnv()
  const { PRIVATE_KEY: privateKey } = Config

  const dealQueue = createDealQueue(db)
  const databaseView = createView(db)

  const item = sqsEvent.Records[0].body
  const { error } = await submissionWorkflow.buildOffer({
    item,
    dealQueue,
    databaseView,
    did,
    privateKey,
    aggregationServiceConnection: getAggregationServiceConnection({
      did: aggregationServiceDid,
      url: aggregationServiceUrl
    })
  })
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
function getConsumeEnv () {
  return {
    db: getDbEnv(),
    queueUrl: mustGetEnv('QUEUE_URL'),
    queueRegion: mustGetEnv('QUEUE_REGION'),
  }
}

/**
 * Get Env validating it is set.
 */
function getBuildOfferEnv () {
  return {
    db: getDbEnv(),
    did: mustGetEnv('DID'),
    aggregationServiceDid: mustGetEnv('AGGREGATION_SERVICE_DID'),
    aggregationServiceUrl: mustGetEnv('AGGREGATION_SERVICE_URL'),
  }
}

export const consume = Sentry.AWSLambda.wrapHandler(consumeHandler)
export const build = Sentry.AWSLambda.wrapHandler(buildOfferHandler)