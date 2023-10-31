import * as Sentry from '@sentry/serverless'
import { Table } from 'sst/node/table'

import { createClient as createInclusionProofStoreClient } from '@w3filecoin/core/src/store/aggregator-inclusion-proof-store.js'
import { createClient as createInclusionStoreClient } from '@w3filecoin/core/src/store/aggregator-inclusion-store.js'
import * as aggregatorEvents from '@web3-storage/filecoin-api/aggregator/events'
import { decodeMessage } from '@w3filecoin/core/src/queue/piece-accept-queue.js'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * Get EventRecord from the SQS Event triggering the handler.
 * On piece accept queue message, store inclusion record in store.
 *
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 */
async function handlePieceAcceptMessage (sqsEvent) {
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

  const { ok, error } = await aggregatorEvents.handlePieceAcceptMessage(context, record)
  if (error) {
    return {
      statusCode: 500,
      body: error.message || 'failed to handle piece accept message'
    }
  }

  return {
    statusCode: 200,
    body: ok
  }
}

function getContext () {
  const {
    inclusionStoreTableName,
    inclusionStoreTableRegion,
    inclusionProofStoreBucketName,
    inclusionProofStoreBucketRegion,
  } = getEnv()

  return {
    inclusionStore: createInclusionStoreClient(
      { region: inclusionStoreTableRegion },
      {
        tableName: inclusionStoreTableName,
        inclusionProofStore: createInclusionProofStoreClient(
          { region: inclusionProofStoreBucketRegion },
          { name: inclusionProofStoreBucketName }
        )
      }
    ),
  }
}

/**
 * Get Env validating it is set.
 */
function getEnv () {
  return {
    inclusionStoreTableName: Table['aggregator-inclusion-store'].tableName,
    inclusionStoreTableRegion: mustGetEnv('AWS_REGION'),
    inclusionProofStoreBucketName: mustGetEnv('INCLUSION_PROOF_STORE_BUCKET_NAME'),
    inclusionProofStoreBucketRegion: mustGetEnv('AWS_REGION'),
  }
}

export const main = Sentry.AWSLambda.wrapHandler(handlePieceAcceptMessage)
