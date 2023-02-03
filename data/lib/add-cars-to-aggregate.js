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

  await aggregateTable.appendCARs(aggregateId, cars)

  return {
    aggregateId
  }
}
