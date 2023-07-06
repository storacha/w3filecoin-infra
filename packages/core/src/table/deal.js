import { Kysely } from 'kysely'

import { getDialect } from './utils.js'
import {
  SQLSTATE_UNIQUE_VALUE_CONSTRAINT_ERROR_CODE
} from './constants.js'
import {
  DatabaseOperationError,
  DatabaseUniqueValueConstraintError
} from './errors.js'

/**
 * @type {Record<string, import('../schema').DealStatus>}
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
 * @param {import('kysely').Kysely<import('../schema').Database>} dbClient
 * @returns {import('../types').DealTable}
 */
export function useDealTable (dbClient) {
  return {
    insert: async (dealItem) => {
      const item = {
        aggregate: `${dealItem.aggregate}`,
        status: STATE.PENDING,
      }

      try {
        await dbClient
          .insertInto(TABLE_NAME)
          .values(item)
          .execute()
      } catch (/** @type {any} */ error) {
        return {
          error: Number.parseInt(error.code) === SQLSTATE_UNIQUE_VALUE_CONSTRAINT_ERROR_CODE ?
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
