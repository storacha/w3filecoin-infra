import * as Sentry from '@sentry/serverless'
import { unmarshall } from '@aws-sdk/util-dynamodb'

import { addCarsToAggregate } from '../lib/add-cars-to-aggregate.js'
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
    AGGREGATE_TABLE_NAME,
    AGGREGATE_MIN_SIZE,
    AGGREGATE_MAX_SIZE
  } = getEnv()

  const records = parseDynamoDbEvent(event)
  // @ts-ignore needs to have unmarshall given records come in dynamodb format
  const cars = records.map(record => unmarshall(record.new))
  const aggregateProps = {
    region: AWS_REGION,
    tableName: AGGREGATE_TABLE_NAME,
    options: {
      minSize: AGGREGATE_MIN_SIZE,
      maxSize: AGGREGATE_MAX_SIZE
    }
  }

  // @ts-expect-error unmarshall does not infer type
  await addCarsToAggregate(cars, aggregateProps)
}

export const consumer = Sentry.AWSLambda.wrapHandler(handler)

/**
 * Get Env validating it is set.
 */
function getEnv() {
  return {
    AGGREGATE_TABLE_NAME: mustGetEnv('AGGREGATE_TABLE_NAME'),
    AGGREGATE_MIN_SIZE: Number(mustGetEnv('AGGREGATE_MIN_SIZE')),
    AGGREGATE_MAX_SIZE: Number(mustGetEnv('AGGREGATE_MAX_SIZE'))
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
