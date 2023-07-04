import { Kysely } from 'kysely'

import { getDialect } from './utils.js'
import {
  SQLSTATE_UNIQUE_VALUE_CONSTRAINT
} from './constants.js'
import {
  DatabaseOperationError,
  DatabaseUniqueValueConstraintError
} from './errors.js'

export const TABLE_NAME = 'inclusion'

/**
 * @param {import('../types').DialectProps} dialectOpts
 */
export function createInclusionTable (dialectOpts) {
  const dialect = getDialect(dialectOpts)

  const dbClient = new Kysely({
    dialect
  })

  return useInclusionTable(dbClient)
}

/**
 * 
 * @param {import('kysely').Kysely<import('../sql.generated').Database>} dbClient
 * @returns {import('../types').InclusionTable}
 */
export function useInclusionTable (dbClient) {
  return {
    insert: async (inclusionItem) => {
      const inserted = (new Date()).toISOString()
      const item = {
        piece: `${inclusionItem.piece}`,
        priority: inclusionItem.priority || '0',
        inserted,
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
    aggregate: async (inclusionItems, aggregateLink) => {
      let res
      try {
        res = await dbClient
          .updateTable(TABLE_NAME)
          .set({
            aggregate: aggregateLink.toString()
          })
          .where('aggregate', 'is', null)
          .where('piece', 'in', inclusionItems.map(i => i.toString()))
          .execute()
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
