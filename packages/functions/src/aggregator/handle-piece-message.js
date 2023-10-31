import * as Sentry from '@sentry/serverless'
import { Table } from 'sst/node/table'

import { createClient as createPieceStoreClient } from '@w3filecoin/core/src/store/aggregator-piece-store.js'
import * as aggregatorEvents from '@web3-storage/filecoin-api/aggregator/events'
import { decodeMessage } from '@w3filecoin/core/src/queue/piece-queue.js'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * Get EventRecord from the SQS Event triggering the handler.
 * The event contains a piece message to be added to the store.
 *
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 */
async function handlePieceMessage (sqsEvent) {
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

  const { ok, error } = await aggregatorEvents.handlePieceMessage(context, record)
  if (error) {
    return {
      statusCode: 500,
      body: error.message || 'failed to handle piece message'
    }
  }

  return {
    statusCode: 200,
    body: ok
  }
}

function getContext () {
  const {
    pieceStoreTableName,
    pieceStoreTableRegion,
  } = getEnv()

  return {
    pieceStore: createPieceStoreClient(
      { region: pieceStoreTableRegion },
      { tableName: pieceStoreTableName }
    )
  }
}

/**
 * Get Env validating it is set.
 */
function getEnv () {
  return {
    pieceStoreTableName: Table['aggregator-piece-store'].tableName,
    pieceStoreTableRegion: mustGetEnv('AWS_REGION'),
  }
}

export const main = Sentry.AWSLambda.wrapHandler(handlePieceMessage)
