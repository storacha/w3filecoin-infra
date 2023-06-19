import { Kysely } from 'kysely'
import { parse as parseLink } from 'multiformats/link'

import { useCargoTable } from './cargo.js'
import { getDialect } from './utils.js'
import {
  DEFAULT_LIMIT,
  SQLSTATE_UNIQUE_VALUE_CONSTRAINT
} from './constants.js'
import {
  DatabaseOperationError,
  DatabaseUniqueValueConstraintError,
  DatabaseValueToUpdateNotFoundError
} from './errors.js'

/**
 * @type {Record<string, import('../sql.generated').FerryState>}
 */
export const STATE = {
  QUEUED: 'QUEUED',
  ARRANGING: 'ARRANGING',
  FAILED: 'FAILED',
  SUCCEED: 'SUCCEED'
}
export const TABLE_NAME = 'ferry'

/**
 * @param {import('../types').DialectProps} dialectOpts
 */
export function createFerryTable (dialectOpts) {
  const dialect = getDialect(dialectOpts)

  const dbClient = new Kysely({
    dialect
  })

  return useFerryTable(dbClient)
}

/**
 * 
 * @param {import('kysely').Kysely<import('../sql.generated').Database>} dbClient
 * @returns {import('../types').FerryTable}
 */
export function useFerryTable (dbClient) {
  return {
    insert: async (item, cargoItems) => {
      const inserted = (new Date()).toISOString()
      const ferryItem = {
        link: `${item.link}`,
        size: item.size,
        state: STATE.QUEUED,
        inserted,
        priority: item.priority || inserted
      }

      try {
        // Transaction
        await dbClient.transaction().execute(async trx => {
          // Create cargo table backed by transaction client
          const cargoTable = useCargoTable(trx)

          // Insert Ferry
          await trx
            .insertInto(TABLE_NAME)
            .values(ferryItem)
            .execute()
          
          // Load cargo into ferry for offering
          const { error } = await cargoTable.updateCargoOffering(cargoItems, item.link)
          if (error) {
            throw error
          }
        })
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
        ok: res.map(ferry => ({
          ...ferry,
          link: parseLink(ferry.link),
          size: Number.parseInt(ferry.size),
          inserted: ferry.inserted ?? undefined,
        }))
      }
    },
    updateFerryToArranging: async (ferryItem) => {
      let res
      try {
        res = await dbClient
          .updateTable(TABLE_NAME)
          .set({
            state: STATE.ARRANGING
          })
          .where('state', '=', STATE.QUEUED)
          .where('link', '=', ferryItem.toString())
          .executeTakeFirstOrThrow()
      } catch (/** @type {any} */ error) {
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      if (res.numUpdatedRows === BigInt(0)) {
        return {
          error: new DatabaseValueToUpdateNotFoundError(`${ferryItem.link} in ${STATE.QUEUED} state not found`)
        }
      }
      return {
        ok: {}
      }
    },
    updateFerryToSucceed: async (ferryItem) => {
      try {
        // Transaction
        await dbClient.transaction().execute(async trx => {
          // Create cargo table backed by transaction client
          const cargoTable = useCargoTable(trx)

          // Update Ferry
          const resUpdateFerry = await trx
            .updateTable(TABLE_NAME)
            .set({
              state: STATE.SUCCEED
            })
            .where('state', '=', STATE.ARRANGING)
            .where('link', '=', ferryItem.toString())
            .executeTakeFirstOrThrow()

          // Fail if not in expected state
          if (resUpdateFerry.numUpdatedRows === BigInt(0)) {
            throw new DatabaseValueToUpdateNotFoundError(`${ferryItem.link} in ${STATE.ARRANGING} state not found`)
          }
          
          // update cargo to reflect its new success state
          const { error } = await cargoTable.updateCargoSuccess(ferryItem)
          if (error) {
            throw error
          }
        })
      } catch (/** @type {any} */ error) {
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      return {
        ok: {}
      }
    },
    updateFerryToFailed: async (ferryItem, failedCargoItems) => {
      // Create cargo table backed by client
      // We already create a transaction here, so we cannot pass the transaction itself
      const cargoTable = useCargoTable(dbClient)

      try {
        // Transaction
        await dbClient.transaction().execute(async trx => {
          // Update Ferry
          const resUpdateFerry = await trx
            .updateTable(TABLE_NAME)
            .set({
              state: STATE.FAILED
            })
            .where('state', '=', STATE.ARRANGING)
            .where('link', '=', ferryItem.toString())
            .executeTakeFirstOrThrow()

          // Fail if not in expected state
          if (resUpdateFerry.numUpdatedRows === BigInt(0)) {
            throw new DatabaseValueToUpdateNotFoundError(`${ferryItem.link} in ${STATE.ARRANGING} state not found`)
          }

          // update cargo to reflect its new success state
          const { error } = await cargoTable.updateCargoFailedOrQueuedOnTrx(ferryItem, failedCargoItems, trx)
          if (error) {
            throw error
          }
        })
      } catch (/** @type {any} */ error) {
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      return {
        ok: {}
      }
    }
  }
}
