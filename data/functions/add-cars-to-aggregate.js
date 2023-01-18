import * as Sentry from '@sentry/serverless'
import { unmarshall } from '@aws-sdk/util-dynamodb'

import { createRedis } from '../redis.js'
import { createAggregateTable } from '../tables/aggregate.js'
import { parseDynamoDbEvent } from '../utils/parse-dynamodb-event.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

const AWS_REGION = process.env.AWS_REGION || 'us-west-2'

/**
 * @param {import('aws-lambda').DynamoDBStreamEvent} event
 */
async function handler(event) {
  const {
    AGGREGATE_TABLE_NAME,
    AGGREGATE_MIN_SIZE,
    AGGREGATE_MAX_SIZE,
    REDIS_URL,
    REDIS_PORT,
    REDIS_KEY,
  } = getEnv()

  const records = parseDynamoDbEvent(event)
  // TODO: filter with unique needed?
  const cars = records.map(record => record.new)

  const aggregateTable = createAggregateTable(AWS_REGION, AGGREGATE_TABLE_NAME, {
    minSize: AGGREGATE_MIN_SIZE,
    maxSize: AGGREGATE_MAX_SIZE
  })
  const redis = createRedis(REDIS_URL, REDIS_PORT)

  const aggregateId = await redis.get(REDIS_KEY)
  if (!aggregateId) {
    throw new Error('aggregate id must exist!')
  }

  // @ts-ignore needs to have unmarshall given records come in dynamodb format
  await aggregateTable.add(aggregateId, cars.map(unmarshall))
}

export const consumer = Sentry.AWSLambda.wrapHandler(handler)

/**
 * Get Env validating it is set.
 */
function getEnv() {
  return {
    AGGREGATE_TABLE_NAME: mustGetEnv('AGGREGATE_TABLE_NAME'),
    AGGREGATE_MIN_SIZE: Number(mustGetEnv('AGGREGATE_MIN_SIZE')),
    AGGREGATE_MAX_SIZE: Number(mustGetEnv('AGGREGATE_MAX_SIZE')),
    REDIS_URL: mustGetEnv('REDIS_URL'),
    REDIS_PORT: Number(mustGetEnv('REDIS_PORT')),
    REDIS_KEY: mustGetEnv('REDIS_KEY')
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
