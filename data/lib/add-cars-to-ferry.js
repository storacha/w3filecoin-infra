import { createFerryTable } from '../tables/ferry.js'

/**
 * @typedef {import('../types').FerryOpts} FerryOpts
 * @typedef {import('../types').CarItemFerry} CarItem
 *
 * @typedef {object} FerryProps
 * @property {string} region
 * @property {string} tableName
 * @property {FerryOpts} [options]
 */

/**
 * Add cars to a loading ferry.
 *
 * @param {CarItem[]} cars
 * @param {FerryProps} ferryProps
 */
export async function addCarsToFerry (cars, ferryProps) {
  const ferryTable = createFerryTable(ferryProps.region, ferryProps.tableName, ferryProps.options)

  let ferryId = await ferryTable.getFerryLoading()
  if (!ferryId) {
    // Set new one if no ferry is loading
    ferryId = `${Date.now()}`
  }

  await ferryTable.addCargo(ferryId, cars)

  return {
    id: ferryId
  }
}
