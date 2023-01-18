import * as Sentry from '@sentry/serverless'
import { unmarshall } from '@aws-sdk/util-dynamodb'

import { createRedis,  } from '../redis.js'
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
  if (records.length > 1) {
    throw new Error('todo name')
  }

  // @ts-expect-error can't figure out type of new
  const newRecord = unmarshall(records[0].new)

  // Still not ready - TODO this should be handled by a filter when supported
  if (newRecord.size < AGGREGATE_MIN_SIZE) {
    console.log(`aggregate still not ready: ${newRecord.size} for a minimum of ${AGGREGATE_MIN_SIZE}`)
    return
  }

  const currentAggregateId = newRecord.aggregateId
  const redis = createRedis(REDIS_URL, REDIS_PORT)
  const aggregateTable = createAggregateTable(AWS_REGION, AGGREGATE_TABLE_NAME, {
    minSize: AGGREGATE_MIN_SIZE,
    maxSize: AGGREGATE_MAX_SIZE
  })

  let hadConcurrentError = false
  // Update state
  try {
    await aggregateTable.setAsReady(currentAggregateId)
  } catch (/** @type {any} */ error) {
    // If error is for condition we can safely ignore it given this was changed in a concurrent operation
    if (error.name !== 'ConditionalCheckFailedException') {
      throw error
    }
    hadConcurrentError = true
    console.log('dynamo error', error.name)
  }

  // Define update aggregate command
  const messageError = 'aggregate key changed!'
  const updateScript = `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('set', KEYS[1], ARGV[2]) else return redis.error_reply("${messageError}") end`
  redis.defineCommand('updateAggregate', {
    numberOfKeys: 1,
    lua: updateScript,
  })

  let newAggregateId
  try {
    newAggregateId = Date.now()
    // @ts-expect-error
    await redis.updateAggregate(REDIS_KEY, newRecord.aggregateId, newAggregateId)
  } catch (/** @type {any} */ error) {
    // If error is for condition we can safely ignore it given this was changed in a concurrent operation
    if (error.message !== messageError) {
      throw error
    }
    hadConcurrentError = true
    console.log('redis error', error.message)
  }

  // LOG
  if (!hadConcurrentError) {
    console.log(`Aggregate ${currentAggregateId} is Ready. Starting aggregate ${newAggregateId}`)
  }
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
