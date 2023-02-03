import * as Sentry from '@sentry/serverless'
import { unmarshall } from '@aws-sdk/util-dynamodb'

import { addCarsToFerry } from '../lib/add-cars-to-ferry.js'
import { parseDynamoDbEvent } from '../utils/parse-dynamodb-event.js'

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
    CARGO_TABLE_NAME
  } = getEnv()

  const records = parseDynamoDbEvent(event)
  // @ts-ignore needs to have unmarshall given records come in dynamodb format
  const cars = records.map(record => unmarshall(record.new))
  const ferryProps = {
    region: AWS_REGION,
    tableName: FERRY_TABLE_NAME,
    options: {
      cargoTableName: CARGO_TABLE_NAME,
      minSize: FERRY_CARGO_MIN_SIZE,
      maxSize: FERRY_CARGO_MAX_SIZE
    }
  }

  // @ts-expect-error unmarshall does not infer type
  await addCarsToFerry(cars, ferryProps)
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
    CARGO_TABLE_NAME: mustGetEnv('CARGO_TABLE_NAME')
  }
}

/**
 * 
 * @param {string} name 
 * @returns {string}
 */
function mustGetEnv (name) {
  if (!process.env[name]) {
    throw new Error(`Missing env var: ${name}`)
  }

  // @ts-expect-error there will always be a string there, but typescript does not believe
  return process.env[name]
}
