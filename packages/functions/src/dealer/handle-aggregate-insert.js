import * as Sentry from '@sentry/serverless'
import { unmarshall } from '@aws-sdk/util-dynamodb'

import { decodeRecord } from '@w3filecoin/core/src/store/dealer-aggregate-store.js'
import { createClient as createOfferStoreClient } from '@w3filecoin/core/src/store/dealer-offer-store.js'
import * as dealerEvents from '@web3-storage/filecoin-api/dealer/events'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * @typedef {import('@w3filecoin/core/src/store/types').DealerAggregateStoreRecord} DealerAggregateStoreRecord
 */

/**
 * @param {import('aws-lambda').DynamoDBStreamEvent} event
 */
async function handleEvent (event) {
  // Construct context
  const {
    offerStoreBucketName,
    offerStoreBucketRegion
  } = getEnv()

  const context = {
    offerStore: createOfferStoreClient({
      region: offerStoreBucketRegion
    }, {
      name: offerStoreBucketName
    })
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

  const { ok, error } = await dealerEvents.handleAggregateInsert(context, record)
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
    offerStoreBucketName: mustGetEnv('OFFER_STORE_BUCKET_NAME'),
    offerStoreBucketRegion: mustGetEnv('OFFER_STORE_BUCKET_REGION'),
  }
}

export const main = Sentry.AWSLambda.wrapHandler(handleEvent)
