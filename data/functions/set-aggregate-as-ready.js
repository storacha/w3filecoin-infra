import * as Sentry from '@sentry/serverless'
import { unmarshall } from '@aws-sdk/util-dynamodb'

import { setAggregateAsReady } from '../index.js'
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
  if (records.length > 1) {
    throw new Error('Should only receive one aggregate to update')
  }

  // @ts-expect-error can't figure out type of new
  const newRecord = unmarshall(records[0].new)

  // Still not ready - TODO this should be handled by a filter when supported
  if (newRecord.size < AGGREGATE_MIN_SIZE) {
    console.log(`aggregate still not ready: ${newRecord.size} for a minimum of ${AGGREGATE_MIN_SIZE}`)
    return
  }

  const aggregateProps = {
    region: AWS_REGION,
    tableName: AGGREGATE_TABLE_NAME,
    options: {
      minSize: AGGREGATE_MIN_SIZE,
      maxSize: AGGREGATE_MAX_SIZE
    }
  }
  const redisProps = {
    url: REDIS_URL,
    port: REDIS_PORT,
    key: REDIS_KEY,
    options: {
      tls: {}
    }
  }

  // @ts-expect-error unmarshall does not infer type
  await setAggregateAsReady(newRecord, aggregateProps, redisProps)
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
    REDIS_KEY: mustGetEnv('REDIS_KEY'),
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
