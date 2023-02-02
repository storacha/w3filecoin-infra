import { createAggregateTable } from './tables/aggregate.js'

/**
 * @typedef {import('./types').AggregateOpts} AggregateOpts
 * @typedef {import('./types').CarItemAggregate} CarItem
 *
 * @typedef {object} AggregateProps
 * @property {string} region
 * @property {string} tableName
 * @property {AggregateOpts} [options]
 */

/**
 * Add cars to current aggregate.
 *
 * @param {CarItem[]} cars
 * @param {AggregateProps} aggregateProps
 */
export async function addCarsToAggregate (cars, aggregateProps) {
  const aggregateTable = createAggregateTable(aggregateProps.region, aggregateProps.tableName, aggregateProps.options)

  let aggregateId = await aggregateTable.getAggregateIngesting()
  if (!aggregateId) {
    // Set new one if no ingesting aggregate
    aggregateId = `${Date.now()}`
  }

  await aggregateTable.add(aggregateId, cars)

  return {
    aggregateId
  }
}

/**
 * Sets current aggregate as ready if not previously done
 *
 * @param {{ aggregateId: string; }} aggregateRecord
 * @param {AggregateProps} aggregateProps
 */
export async function setAggregateAsReady (aggregateRecord, aggregateProps) {
  const currentAggregateId = aggregateRecord.aggregateId
  const aggregateTable = createAggregateTable(aggregateProps.region, aggregateProps.tableName, aggregateProps.options)

  await aggregateTable.getAggregateIngesting()
  // Update state
  try {
    await aggregateTable.setAsReady(currentAggregateId)
  } catch (/** @type {any} */ error) {
    // If error is for condition we can safely ignore it given this was changed in a concurrent operation
    if (error.name !== 'ConditionalCheckFailedException') {
      throw error
    }
  }
}
