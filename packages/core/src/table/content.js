import { Kysely } from 'kysely'

import { getDialect } from './utils.js'
import {
  SQLSTATE_UNIQUE_VALUE_CONSTRAINT_ERROR_CODE
} from './constants.js'
import {
  DatabaseOperationError,
  DatabaseUniqueValueConstraintError
} from './errors.js'

export const TABLE_NAME = 'content'

/**
 * @param {import('../types').DialectProps} dialectOpts
 */
export function createContentTable (dialectOpts) {
  const dialect = getDialect(dialectOpts)

  const dbClient = new Kysely({
    dialect
  })

  return useContentTable(dbClient)
}

/**
 * 
 * @param {import('kysely').Kysely<import('../schema').Database>} dbClient
 * @returns {import('../types').ContentTable}
 */
export function useContentTable (dbClient) {
  return {
    insert: async (contentItem) => {
      const item = {
        link: `${contentItem.link}`,
        size: contentItem.size,
        source: JSON.stringify(contentItem.source),
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
