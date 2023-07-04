import { Kysely } from 'kysely'

import { getDialect } from './utils.js'
import {
  SQLSTATE_UNIQUE_VALUE_CONSTRAINT
} from './constants.js'
import {
  DatabaseOperationError,
  DatabaseUniqueValueConstraintError
} from './errors.js'

/**
 * @type {Record<string, import('../sql.generated').DealStatus>}
 */
export const STATE = {
  APPROVED: 'APPROVED',
  PENDING: 'PENDING',
  REJECTED: 'REJECTED',
  SIGNED: 'SIGNED'
}
export const TABLE_NAME = 'deal'

/**
 * @param {import('../types').DialectProps} dialectOpts
 */
export function createDealTable (dialectOpts) {
  const dialect = getDialect(dialectOpts)

  const dbClient = new Kysely({
    dialect
  })

  return useDealTable(dbClient)
}

/**
 * 
 * @param {import('kysely').Kysely<import('../sql.generated').Database>} dbClient
 * @returns {import('../types').DealTable}
 */
export function useDealTable (dbClient) {
  return {
    insert: async (dealItem) => {
      const inserted = (new Date()).toISOString()
      const item = {
        aggregate: `${dealItem.aggregate}`,
        status: STATE.PENDING,
        inserted
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
    }
  }
}
