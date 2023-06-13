import * as Sentry from '@sentry/serverless'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { Config } from '@serverless-stack/node/config/index.js'

import { setFerryOffer } from '../lib/index.js'
import { parseDynamoDbEvent } from '../utils/parse-dynamodb-event.js'
import { mustGetEnv, getAggregationServiceConnection } from '../lib/utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

const AWS_REGION = mustGetEnv('AWS_REGION')

/**
 * @param {import('aws-lambda').DynamoDBStreamEvent} event
 */
async function handler(event) {
  const {
    CAR_TABLE_NAME,
    CARGO_TABLE_NAME,
    FERRY_TABLE_NAME,
    DID,
    AGGREGATION_SERVICE_DID,
    AGGREGATION_SERVICE_URL,
  } = getEnv()
  const { PRIVATE_KEY } = Config

  const records = parseDynamoDbEvent(event)
  if (records.length > 1) {
    throw new Error('Should only receive one ferry to update')
  }

  // @ts-expect-error can't figure out type of new
  const newRecord = unmarshall(records[0].new)

  const ctx = {
    car: {
      region: AWS_REGION,
      tableName: CAR_TABLE_NAME
    },
    ferry: {
      region: AWS_REGION,
      tableName: FERRY_TABLE_NAME,
      options: {
        cargoTableName: CARGO_TABLE_NAME
      }
    },
    storefront: {
      DID,
      PRIVATE_KEY
    },
    aggregationServiceConnection: getAggregationServiceConnection({
      DID: AGGREGATION_SERVICE_DID,
      URL: AGGREGATION_SERVICE_URL
    })
  }
  await setFerryOffer(newRecord.id, ctx)
}

export const consumer = Sentry.AWSLambda.wrapHandler(handler)

/**
 * Get Env validating it is set.
 */
function getEnv() {
  return {
    DID: mustGetEnv('DID'),
    CAR_TABLE_NAME: mustGetEnv('CAR_TABLE_NAME'),
    CARGO_TABLE_NAME: mustGetEnv('CARGO_TABLE_NAME'),
    FERRY_TABLE_NAME: mustGetEnv('FERRY_TABLE_NAME'),
    AGGREGATION_SERVICE_DID: mustGetEnv('AGGREGATION_SERVICE_DID'),
    AGGREGATION_SERVICE_URL: mustGetEnv('AGGREGATION_SERVICE_URL'),
  }
}
