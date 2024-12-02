import * as Sentry from '@sentry/serverless'
import { Config } from 'sst/node/config'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import * as Delegation from '@ucanto/core/delegation'
import { fromString } from 'uint8arrays/from-string'
import * as DID from '@ipld/dag-ucan/did'

import { getServiceConnection, getServiceSigner } from '@w3filecoin/core/src/service.js'
import { decodeRecord } from '@w3filecoin/core/src/store/aggregator-aggregate-store.js'
import { createClient as createBufferStoreClient } from '@w3filecoin/core/src/store/aggregator-buffer-store.js'
import * as aggregatorEvents from '@web3-storage/filecoin-api/aggregator/events'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 0,
})

/**
 * @typedef {import('@web3-storage/filecoin-api/aggregator/api').AggregateRecord} AggregateRecord
 * @typedef {import('@w3filecoin/core/src/store/types').InferStoreRecord<AggregateRecord>} InferStoreRecord
 */

/**
 * Get EventRecord from the SQS Event triggering the handler.
 * On Aggregate store insert, offer inserted aggregate for deal.
 *
 * @param {import('aws-lambda').DynamoDBStreamEvent} event
 */
async function handleAggregateInsertToAggregateOffer (event) {
  // Parse records
  const eventRawRecords = parseDynamoDbEvent(event)
  // if one we should put back in queue
  if (eventRawRecords.length !== 1) {
    return {
      statusCode: 400,
      body: `Expected 1 DynamoDBStreamEvent per invocation but received ${eventRawRecords.length}`
    }
  }
  /** @type {InferStoreRecord} */
  // @ts-expect-error can't figure out type of new
  const storeReecord = unmarshall(eventRawRecords[0].new)
  const record = decodeRecord(storeReecord)

  // Get context
  const context = await getContext()
  const { ok, error } = await aggregatorEvents.handleAggregateInsertToAggregateOffer(context, record)
  if (error) {
    return {
      statusCode: 500,
      body: error.message || 'failed to handle aggregate insert event'
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

async function getContext () {
  const {
    did,
    serviceDid,
    delegatedProof,
    serviceUrl,
    bufferStoreBucketName,
    bufferStoreBucketRegion,
  } = getEnv()

  const { AGGREGATOR_PRIVATE_KEY: privateKey } = Config
  let issuer = getServiceSigner({
    privateKey
  })
  const connection = getServiceConnection({
    did: serviceDid,
    url: serviceUrl
  })
  const proofs = []
  if (delegatedProof) {
    const proof = await Delegation.extract(fromString(delegatedProof, 'base64pad'))
      if (!proof.ok) throw new Error('failed to extract proof', { cause: proof.error })
      proofs.push(proof.ok)
  } else {
    // if no proofs, we must be using the service private key to sign
    issuer = issuer.withDID(DID.parse(did).did())
  }

  return {
    bufferStore: createBufferStoreClient(
      { region: bufferStoreBucketRegion },
      { name: bufferStoreBucketName }
    ),
    dealerService: {
      connection,
      invocationConfig: {
        issuer,
        audience: connection.id,
        with: issuer.did()
      }
    },
  }
}

/**
 * Get Env validating it is set.
 */
function getEnv () {
  return {
    did: mustGetEnv('DID'),
    serviceDid: mustGetEnv('SERVICE_DID'),
    serviceUrl: mustGetEnv('SERVICE_URL'),
    delegatedProof: process.env.PROOF,
    bufferStoreBucketName: mustGetEnv('BUFFER_STORE_BUCKET_NAME'),
    bufferStoreBucketRegion: mustGetEnv('AWS_REGION'),
  }
}

export const main = Sentry.AWSLambda.wrapHandler(handleAggregateInsertToAggregateOffer)
