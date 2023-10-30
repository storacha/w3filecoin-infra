import * as Sentry from '@sentry/serverless'
import { Config } from 'sst/node/config'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import * as Delegation from '@ucanto/core/delegation'
import { fromString } from 'uint8arrays/from-string'
import * as DID from '@ipld/dag-ucan/did'

import { getServiceConnection, getServiceSigner } from '@w3filecoin/core/src/service.js'
import { decodeRecord } from '@w3filecoin/core/src/store/dealer-aggregate-store.js'
import * as dealerEvents from '@web3-storage/filecoin-api/dealer/events'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * @typedef {import('@w3filecoin/core/src/store//types').DealerAggregateStoreRecord} DealerAggregateStoreRecord
 */

/**
 * @param {import('aws-lambda').DynamoDBStreamEvent} event
 */
async function handleEvent (event) {
  const {
    did,
    serviceDid,
    delegatedProof,
    serviceUrl
  } = getEnv()
  const { DEALER_PRIVATE_KEY: privateKey } = Config
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

  const context = {
    dealerService: {
      connection,
      invocationConfig: {
        issuer,
        audience: connection.id,
        with: issuer.did(),
        proofs
      }
    }
  }

  // Parse record
  // Get deal ready for aggregate/accept
  const records = parseDynamoDbEvent(event)
  if (records.length > 1 || !records[0].new) {
    return {
      statusCode: 500,
      body: 'Should only receive one aggregate to handle'
    }
  }
  // @ts-expect-error can't figure out type of new
  const aggregateStoreRecord = /** @type {DealerAggregateStoreRecord} */ (unmarshall(records[0].new))
  const record = decodeRecord(aggregateStoreRecord)

  // @ts-expect-error weirdness type error for ucanto/interface until we get rid of legacy deps
  // Types of parameters 'request' and 'request' are incompatible.
  const { ok, error } = await dealerEvents.handleAggregateUpdatedStatus(context, record)
  if (error) {
    return {
      statusCode: 500,
      body: error.message || 'failed to handle aggregate insert'
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

/**
 * Get Env validating it is set.
 */
function getEnv () {
  return {
    did: mustGetEnv('DID'),
    serviceDid: mustGetEnv('SERVICE_URL'),
    serviceUrl: mustGetEnv('SERVICE_URL'),
    delegatedProof: mustGetEnv('PROOF'),
  }
}

export const main = Sentry.AWSLambda.wrapHandler(handleEvent)
