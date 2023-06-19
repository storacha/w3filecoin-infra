import { Kysely } from 'kysely'
import { parse as parseLink } from 'multiformats/link'

import { getDialect } from './utils.js'
import {
  DEFAULT_LIMIT,
  SQLSTATE_UNIQUE_VALUE_CONSTRAINT
} from './constants.js'
import {
  DatabaseOperationError,
  DatabaseUniqueValueConstraintError
} from './errors.js'

/**
 * @type {Record<string, import('../sql.generated').CargoState>}
 */
export const STATE = {
  QUEUED: 'QUEUED',
  OFFERING: 'OFFERING',
  FAILED: 'FAILED',
  SUCCEED: 'SUCCEED'
}
export const TABLE_NAME = 'cargo'

/**
 * @param {import('../types').DialectProps} dialectOpts
 */
export function createCargoTable (dialectOpts) {
  const dialect = getDialect(dialectOpts)

  const dbClient = new Kysely({
    dialect
  })

  return useCargoTable(dbClient)
}

/**
 * 
 * @param {import('kysely').Kysely<import('../sql.generated').Database>} dbClient
 * @returns {import('../types').CargoTable}
 */
export function useCargoTable (dbClient) {
  return {
    insert: async (cargoItem) => {
      const inserted = (new Date()).toISOString()
      const item = {
        link: `${cargoItem.link}`,
        size: cargoItem.size,
        car_link: `${cargoItem.carLink.toString()}`,
        state: STATE.QUEUED,
        inserted,
        priority: cargoItem.priority || inserted
      }

      try {
        await dbClient
          .insertInto(TABLE_NAME)
          .values(item)
          .execute()
      } catch (/** @type {any} */ error) {
        return {
          error: Number.parseInt(error.code) === SQLSTATE_UNIQUE_VALUE_CONSTRAINT ?
            new DatabaseUniqueValueConstraintError(error.message) :
            new DatabaseOperationError(error.message)
        }
      }

      return {
        ok: {}
      }
    },
    selectByState: async (state, options = {}) => {
      const limit = options.limit || DEFAULT_LIMIT
      const orderBy = options.orderBy || 'priority'

      let res
      try {
        res = await dbClient
          .selectFrom(TABLE_NAME)
          .selectAll()
          .where('state', '=', state)
          .orderBy(orderBy)
          .limit(limit)
          .execute() 
      } catch (/** @type {any} */ error) {
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      return {
        ok: res.map(cargo => ({
          ...cargo,
          link: parseLink(cargo.link),
          carLink: parseLink(cargo.car_link),
          size: Number.parseInt(cargo.size),
          inserted: cargo.inserted ?? undefined,
          ferryLink: cargo.ferry_link !== null ? parseLink(cargo.ferry_link) : undefined,
          ferryFailedCode: cargo.ferry_failed_code ?? undefined
        }))
      }
    },
    updateCargoOffering: async (cargoItems, ferryLink) => {
      let res
      try {
        res = await dbClient
          .updateTable(TABLE_NAME)
          .set({
            state: STATE.OFFERING,
            ferry_link: ferryLink.toString()
          })
          .where('state', '=', STATE.QUEUED)
          .where('link', 'in', cargoItems.map(i => i.toString()))
          .execute()
      } catch (/** @type {any} */ error) {
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      return {
        ok: res
      }
    },
    updateCargoSuccess: async (ferryLink) => {
      let res
      try {
        res = await dbClient
          .updateTable(TABLE_NAME)
          .set({
            state: STATE.SUCCEED,
          })
          .where('state', '=', STATE.OFFERING)
          .where('ferry_link', '=', ferryLink.toString())
          .execute()
      } catch (/** @type {any} */ error) {
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      return {
        ok: res
      }
    },
    updateCargoFailedOrQueuedOnTrx: async (ferryLink, failedCargoItems, trx) => {
      let res
      try {
        // This function must be performed as part of a transaction already in place
        // We cannot create a transaction inside a transaction
        res = await Promise.all([
          // Update failed items
          ...(failedCargoItems.map(async failedItem => {
            return trx
              .updateTable(TABLE_NAME)
              .set({
                state: STATE.FAILED,
                ferry_failed_code: failedItem.code
              })
              .where('state', '=', STATE.OFFERING)
              .where('ferry_link', '=', ferryLink.toString())
              .where('link', 'in', failedCargoItems.map(i => i.link.toString()))
              .execute()
          })),
          // Update items to queue
          (async () => {
            return trx
              .updateTable(TABLE_NAME)
              .set({
                state: STATE.QUEUED,
              })
              .where('state', '=', STATE.OFFERING)
              .where('ferry_link', '=', ferryLink.toString())
              .where('link', 'not in', failedCargoItems.map(i => i.link.toString()))
              .execute()
          })()
        ])
      } catch (/** @type {any} */ error) {
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      return {
        ok: res
      }
    }
  }
}

