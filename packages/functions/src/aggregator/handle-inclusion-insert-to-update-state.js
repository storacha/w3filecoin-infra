import * as Sentry from '@sentry/serverless'
import { Table } from 'sst/node/table'
import { unmarshall } from '@aws-sdk/util-dynamodb'

import { decodeRecord } from '@w3filecoin/core/src/store/aggregator-inclusion-store.js'
import { createClient as createPieceStoreClient } from '@w3filecoin/core/src/store/aggregator-piece-store.js'
import * as aggregatorEvents from '@web3-storage/filecoin-api/aggregator/events'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 0,
})

/**
 * @typedef {import('@w3filecoin/core/src/store/types').AggregatorInclusionRecord} AggregatorInclusionRecord
 * @typedef {import('@w3filecoin/core/src/store/types').AggregatorInclusionStoreRecord} AggregatorInclusionStoreRecord
 */

/**
 * Get EventRecord from the SQS Event triggering the handler.
 * On Inclusion store insert, piece table can be updated to reflect piece state.
 *
 * @param {import('aws-lambda').DynamoDBStreamEvent} event
 */
async function handleInclusionInsertToUpdateState (event) {
  // Get context
  const context = getContext()

  // Parse records
  const eventRawRecords = parseDynamoDbEvent(event)
  // if one we should put back in queue
  if (eventRawRecords.length !== 1) {
    return {
      statusCode: 400,
      body: `Expected 1 DynamoDBStreamEvent per invocation but received ${eventRawRecords.length}`
    }
  }
  /** @type {AggregatorInclusionStoreRecord} */
  // @ts-expect-error can't figure out type of new
  const storeRecord = unmarshall(eventRawRecords[0].new)
  const record = decodeRecord(storeRecord)

  const { ok, error } = await aggregatorEvents.handleInclusionInsertToUpdateState(context, record)
  if (error) {
    return {
      statusCode: 500,
      body: error.message || 'failed to handle inclusion insert event'
    }
  }

  return {
    statusCode: 200,
    body: ok
  }
}

/**
 * @param {import('aws-lambda').DynamoDBStreamEvent} event
 */
function parseDynamoDbEvent (event) {
  return event.Records.map(r => ({
    new: r.dynamodb?.NewImage,
    old: r.dynamodb?.OldImage
  }))
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

export const main = Sentry.AWSLambda.wrapHandler(handleInclusionInsertToUpdateState)
