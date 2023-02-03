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

  const ferryId = await ferryTable.getFerryLoading()
  await ferryTable.addCargo(ferryId, cars)

  return {
    id: ferryId
  }
}
