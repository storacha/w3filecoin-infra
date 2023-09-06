import * as Sentry from '@sentry/serverless'
import { Table } from 'sst/node/table'
import { Config } from 'sst/node/config'

import { getServiceConnection, getServiceSigner } from '@w3filecoin/core/src/service.js'
import { createTableStoreClient } from '@w3filecoin/core/src/store/table-client.js'
import { createBucketStoreClient } from '@w3filecoin/core/src/store/bucket-client.js'
import { encode as bufferEncode, decode as bufferDecode } from '@w3filecoin/core/src/data/buffer.js'
import { encode as aggregateEncode, decode as aggregateDecode } from '@w3filecoin/core/src/data/aggregate.js'
import { dealerQueue } from '@w3filecoin/core/src/workflow/dealer-queue.js'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * Get EventRecord from the SQS Event triggering the handler.
 * The event contains a batch of `aggregate`(s) provided from `buffer-queue`. These
 * aggregates should be stored and added to the dealer.
 *
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 */
async function dealerQueueWorkflow (sqsEvent) {
  if (sqsEvent.Records.length !== 1) {
    return {
      statusCode: 400,
      body: `Expected 1 sqsEvent per invocation but received ${sqsEvent.Records.length}`
    }
  }

  const { bufferStoreClient, aggregateStoreClient } = getProps()
  const { did, dealerDid, dealerUrl } = getEnv()
  const { PRIVATE_KEY: privateKey } = Config
  const aggregateRecord = sqsEvent.Records[0].body

  const dealerServiceConnection = getServiceConnection({
    did: dealerDid,
    url: dealerUrl
  })
  const issuer = getServiceSigner({
    did,
    privateKey
  })
  const audience = dealerServiceConnection.id
  /** @type {import('@web3-storage/filecoin-client/types').InvocationConfig} */
  const invocationConfig = {
    issuer,
    audience,
    with: issuer.did(),
  }

  const { ok, error } = await dealerQueue({
    bufferStoreClient,
    aggregateStoreClient,
    aggregateRecord,
    dealerServiceConnection,
    invocationConfig
  })

  if (error) {
    return {
      statusCode: 500,
      body: error.message || 'failed to add aggregate'
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
  const { bufferStoreBucketName, bufferStoreBucketRegion, aggregateStoreTableName, aggregateStoreTableRegion } = getEnv()

  return {
    bufferStoreClient: createBucketStoreClient({
      region: bufferStoreBucketRegion
    }, {
      name: bufferStoreBucketName,
      encodeRecord: bufferEncode.storeRecord,
      decodeRecord: bufferDecode.storeRecord,
    }),
    aggregateStoreClient: createTableStoreClient({
      region: aggregateStoreTableRegion
    }, {
      tableName: aggregateStoreTableName.tableName,
      encodeRecord: aggregateEncode.storeRecord,
      decodeRecord: aggregateDecode.storeRecord,
      encodeKey: aggregateEncode.storeKey
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
    aggregateStoreTableName: Table['aggregate-store'],
    aggregateStoreTableRegion: mustGetEnv('AWS_REGION'),
    did: mustGetEnv('DID'),
    dealerDid: mustGetEnv('DEALER_DID'),
    dealerUrl: mustGetEnv('DEALER_URL'),
  }
}

export const workflow = Sentry.AWSLambda.wrapHandler(dealerQueueWorkflow)
