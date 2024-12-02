import * as Sentry from '@sentry/serverless'
import { Config } from 'sst/node/config'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import * as Delegation from '@ucanto/core/delegation'
import { fromString } from 'uint8arrays/from-string'
import * as DID from '@ipld/dag-ucan/did'

import { getServiceConnection, getServiceSigner } from '@w3filecoin/core/src/service.js'
import { decodeRecord } from '@w3filecoin/core/src/store/aggregator-inclusion-store.js'
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
 * On Inclusion store insert, piece/accept can be invoked.
 *
 * @param {import('aws-lambda').DynamoDBStreamEvent} event
 */
async function handleInclusionInsertToIssuePieceAccept (event) {
  // Get context
  const context = await getContext()

  // Parse records
  const eventRawRecords = parseDynamoDbEvent(event)
  // if one we should put back in queue
  if (eventRawRecords.length !== 1) {
    return {
      statusCode: 400,
      body: `Expected 1 DynamoDB record per invocation but received ${eventRawRecords.length}`
    }
  }
  /** @type {AggregatorInclusionStoreRecord} */
  // @ts-expect-error can't figure out type of new
  const storeRecord = unmarshall(eventRawRecords[0].new)
  const record = decodeRecord(storeRecord)
  const { ok, error } = await aggregatorEvents.handleInclusionInsertToIssuePieceAccept(context, record)
  if (error) {
    return {
      statusCode: 500,
      body: error.message || 'failed to handle piece inclusion event'
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
    aggregatorService: {
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
  }
}

export const main = Sentry.AWSLambda.wrapHandler(handleInclusionInsertToIssuePieceAccept)
