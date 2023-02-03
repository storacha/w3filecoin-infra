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
 * Sets current Ferry as ready if not previously done
 *
 * @param {string} ferryId
 * @param {FerryProps} ferryProps
 */
export async function setFerryAsReady (ferryId, ferryProps) {
  const ferryTable = createFerryTable(ferryProps.region, ferryProps.tableName, ferryProps.options)

  // Update state of ferry to ready
  try {
    await ferryTable.setAsReady(ferryId)
  } catch (/** @type {any} */ error) {
    // If error is for condition we can safely ignore it given this was changed in a concurrent operation
    if (error.name !== 'ConditionalCheckFailedException') {
      throw error
    }
  }
}
