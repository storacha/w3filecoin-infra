import * as Sentry from '@sentry/serverless'
import { unmarshall } from '@aws-sdk/util-dynamodb'

import { setFerryAsReady } from '../lib/index.js'
import { parseDynamoDbEvent } from '../utils/parse-dynamodb-event.js'
import { mustGetEnv } from '../lib/utils.js'

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
    FERRY_TABLE_NAME,
    FERRY_CARGO_MIN_SIZE,
    FERRY_CARGO_MAX_SIZE,
  } = getEnv()

  const records = parseDynamoDbEvent(event)
  if (records.length > 1) {
    throw new Error('Should only receive one ferry to update')
  }

  // @ts-expect-error can't figure out type of new
  const newRecord = unmarshall(records[0].new)

  // Still not ready - TODO this should be handled by a filter when supported
  if (newRecord.size < FERRY_CARGO_MIN_SIZE) {
    console.log(`ferry not ready: ${newRecord.size} < ${FERRY_CARGO_MIN_SIZE}`)
    return
  }

  const ferryProps = {
    region: AWS_REGION,
    tableName: FERRY_TABLE_NAME,
    options: {
      minSize: FERRY_CARGO_MIN_SIZE,
      maxSize: FERRY_CARGO_MAX_SIZE
    }
  }

  await setFerryAsReady(newRecord.id, ferryProps)
}

export const consumer = Sentry.AWSLambda.wrapHandler(handler)

/**
 * Get Env validating it is set.
 */
function getEnv() {
  return {
    FERRY_TABLE_NAME: mustGetEnv('FERRY_TABLE_NAME'),
    FERRY_CARGO_MIN_SIZE: Number(mustGetEnv('FERRY_CARGO_MIN_SIZE')),
    FERRY_CARGO_MAX_SIZE: Number(mustGetEnv('FERRY_CARGO_MAX_SIZE')),
  }
}
