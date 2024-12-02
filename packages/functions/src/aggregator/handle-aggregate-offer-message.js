import * as Sentry from '@sentry/serverless'
import { Table } from 'sst/node/table'

import { createClient as createAggregateStoreClient } from '@w3filecoin/core/src/store/aggregator-aggregate-store.js'
import * as aggregatorEvents from '@web3-storage/filecoin-api/aggregator/events'
import { decodeMessage } from '@w3filecoin/core/src/queue/aggregate-offer-queue.js'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 0,
})

/**
 * Get EventRecord from the SQS Event triggering the handler.
 * On aggregate offer queue message, store aggregate record in store.
 *
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 */
async function handleAggregateOfferMessage (sqsEvent) {
  if (sqsEvent.Records.length !== 1) {
    return {
      statusCode: 400,
      body: `Expected 1 sqsEvent per invocation but received ${sqsEvent.Records.length}`
    }
  }

  // Get context
  const context = getContext()
  const record = decodeMessage({
    MessageBody: sqsEvent.Records[0].body
  })

  const { ok, error } = await aggregatorEvents.handleAggregateOfferMessage(context, record)
  if (error) {
    return {
      statusCode: 500,
      body: error.message || 'failed to handle aggregate offer message'
    }
  }

  return {
    statusCode: 200,
    body: ok
  }
}

function getContext () {
  const {
    aggregateStoreTableName,
    aggregateStoreTableRegion,
  } = getEnv()

  return {
    aggregateStore: createAggregateStoreClient(
      { region: aggregateStoreTableRegion },
      { tableName: aggregateStoreTableName }
    )
  }
}

/**
 * Get Env validating it is set.
 */
function getEnv () {
  return {
    aggregateStoreTableName: Table['aggregator-aggregate-store'].tableName,
    aggregateStoreTableRegion: mustGetEnv('AWS_REGION'),
  }
}

export const main = Sentry.AWSLambda.wrapHandler(handleAggregateOfferMessage)
