import { createAggregateTable } from '../tables/aggregate.js'

/**
 * @typedef {import('../types').AggregateOpts} AggregateOpts
 * @typedef {import('../types').CarItemAggregate} CarItem
 *
 * @typedef {object} AggregateProps
 * @property {string} region
 * @property {string} tableName
 * @property {AggregateOpts} [options]
 */

/**
 * Sets current aggregate as ready if not previously done
 *
 * @param {string} aggregateId
 * @param {AggregateProps} aggregateProps
 */
export async function setAggregateAsReady (aggregateId, aggregateProps) {
  const aggregateTable = createAggregateTable(aggregateProps.region, aggregateProps.tableName, aggregateProps.options)

  // Update state
  try {
    await aggregateTable.setAsReady(aggregateId)
  } catch (/** @type {any} */ error) {
    // If error is for condition we can safely ignore it given this was changed in a concurrent operation
    if (error.name !== 'ConditionalCheckFailedException') {
      throw error
    }
  }
}
