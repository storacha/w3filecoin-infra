import * as Sentry from '@sentry/serverless'
import { Table } from 'sst/node/table'
import { Config } from 'sst/node/config'
import * as Delegation from '@ucanto/core/delegation'
import { fromString } from 'uint8arrays/from-string'
import * as DID from '@ipld/dag-ucan/did'

import { createClient as createAggregateStoreClient } from '@w3filecoin/core/src/store/dealer-aggregate-store.js'
import { getServiceConnection, getServiceSigner } from '@w3filecoin/core/src/service.js'
import * as dealerEvents from '@web3-storage/filecoin-api/dealer/events'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

async function handleEvent () {
  const {
    did,
    serviceDid,
    delegatedProof,
    serviceUrl,
    aggregateStoreTableName,
    aggregateStoreTableRegion,
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
    dealTrackerService: {
      connection,
      invocationConfig: {
        issuer,
        audience: connection.id,
        with: issuer.did()
      }
    },
    aggregateStore: createAggregateStoreClient({
      region: aggregateStoreTableRegion
    }, {
      tableName: aggregateStoreTableName.tableName
    })
  }

  // @ts-expect-error weirdness type error for ucanto/interface until we get rid of legacy deps
  // Types of parameters 'request' and 'request' are incompatible.
  const { ok, error } = await dealerEvents.handleCronTick(context)
  if (error) {
    return {
      statusCode: 500,
      body: error.message || 'failed to handle cron tick'
    }
  }

  return {
    statusCode: 200,
    body: ok
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
    delegatedProof: mustGetEnv('PROOF'),
    aggregateStoreTableName: Table['dealer-aggregate-store'],
    aggregateStoreTableRegion: mustGetEnv('AWS_REGION'),
  }
}

export const main = Sentry.AWSLambda.wrapHandler(handleEvent)
