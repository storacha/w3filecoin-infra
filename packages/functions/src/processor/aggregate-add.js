import * as Sentry from '@sentry/serverless'
import { Bucket } from 'sst/node/bucket'
import { Table } from 'sst/node/table'
import { Config } from 'sst/node/config'

import { getBrokerServiceConnection, getServiceSigner } from '@w3filecoin/core/src/service.js'
import { createTableStoreClient } from '@w3filecoin/core/src/store/table-client.js'
import { createBucketStoreClient } from '@w3filecoin/core/src/store/bucket-client.js'
import { encode as bufferEncode, decode as bufferDecode } from '@w3filecoin/core/src/data/buffer.js'
import { encode as aggregateEncode, decode as aggregateDecode } from '@w3filecoin/core/src/data/aggregate.js'
import { addAggregate } from '@w3filecoin/core/src/workflow/aggregate-add.js'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * Get EventRecord from the SQS Event triggering the handler.
 * The event contains a batch of `aggregate`(s) provided from `buffer-queue`. These
 * aggregates should be stored and added to the broker.
 *
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 */
async function aggregateAddWorkflow (sqsEvent) {
  if (sqsEvent.Records.length !== 1) {
    return {
      statusCode: 400,
      body: `Expected 1 sqsEvent per invocation but received ${sqsEvent.Records.length}`
    }
  }

  const { bufferStoreClient, aggregateStoreClient } = getProps()
  const { did, brokerDid, brokerUrl } = getEnv()
  const { PRIVATE_KEY: privateKey } = Config
  const aggregateRecord = sqsEvent.Records[0].body

  const brokerServiceConnection = getBrokerServiceConnection({
    did: brokerDid,
    url: brokerUrl
  })
  const issuer = getServiceSigner({
    did,
    privateKey
  })
  const audience = brokerServiceConnection.id
  /** @type {import('@web3-storage/filecoin-client/types').InvocationConfig} */
  const invocationConfig = {
    issuer,
    audience,
    with: issuer.did(),
  }

  const { ok, error } = await addAggregate({
    bufferStoreClient,
    aggregateStoreClient,
    aggregateRecord,
    brokerServiceConnection,
    invocationConfig
  })

  if (error) {
    return {
      statusCode: 500,
      // @ts-expect-error ucanto invocation may have {} error type
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
      name: bufferStoreBucketName.bucketName,
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
    bufferStoreBucketName: Bucket['buffer-store'],
    bufferStoreBucketRegion: mustGetEnv('AWS_REGION'),
    aggregateStoreTableName: Table['aggregate-store'],
    aggregateStoreTableRegion: mustGetEnv('AWS_REGION'),
    did: mustGetEnv('DID'),
    brokerDid: mustGetEnv('BROKER_DID'),
    brokerUrl: mustGetEnv('BROKER_URL'),
  }
}

export const workflow = Sentry.AWSLambda.wrapHandler(aggregateAddWorkflow)
