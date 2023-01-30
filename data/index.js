import { createRedis } from './redis.js'
import { createAggregateTable } from './tables/aggregate.js'

/**
 * @typedef {import('./types').AggregateOpts} AggregateOpts
 * @typedef {import('./types').CarItemAggregate} CarItem
 *
 * @typedef {object} AggregateProps
 * @property {string} region
 * @property {string} tableName
 * @property {AggregateOpts} [options]
 * 
 * @typedef {object} RedisProps
 * @property {string} url
 * @property {number} port
 * @property {string} key
 * @property {any} [options]
 */

/**
 * Add cars to current aggregate.
 *
 * @param {CarItem[]} cars
 * @param {AggregateProps} aggregateProps
 * @param {RedisProps} redisProps
 */
export async function addCarsToAggregate (cars, aggregateProps, redisProps) {
  const aggregateTable = createAggregateTable(aggregateProps.region, aggregateProps.tableName, aggregateProps.options)
  const redis = createRedis(redisProps.url, redisProps.port, redisProps.options)

  const aggregateId = await redis.get(redisProps.key)
  if (!aggregateId) {
    throw new Error(`aggregate id must exist for DB key ${redisProps.key}!`)
  }

  await aggregateTable.add(aggregateId, cars)
}

/**
 * Sets current aggregate as ready if not previously done and updates current aggregate ID.
 *
 * @param {{ aggregateId: string; }} aggregateRecord
 * @param {AggregateProps} aggregateProps
 * @param {RedisProps} redisProps
 */
export async function setAggregateAsReady (aggregateRecord, aggregateProps, redisProps) {
  const currentAggregateId = aggregateRecord.aggregateId
  const aggregateTable = createAggregateTable(aggregateProps.region, aggregateProps.tableName, aggregateProps.options)
  const redis = createRedis(redisProps.url, redisProps.port)

  // Update state
  try {
    await aggregateTable.setAsReady(currentAggregateId)
  } catch (/** @type {any} */ error) {
    // If error is for condition we can safely ignore it given this was changed in a concurrent operation
    if (error.name !== 'ConditionalCheckFailedException') {
      throw error
    }
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
    // @ts-expect-error created command does not exist in type
    await redis.updateAggregate(redisProps.key, aggregateRecord.aggregateId, newAggregateId)
  } catch (/** @type {any} */ error) {
    // If error is for condition we can safely ignore it given this was changed in a concurrent operation
    if (error.message !== messageError) {
      throw error
    }
  }
}
