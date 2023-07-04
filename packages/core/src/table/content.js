import { Kysely } from 'kysely'

import { getDialect } from './utils.js'
import {
  SQLSTATE_UNIQUE_VALUE_CONSTRAINT
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
 * @param {import('kysely').Kysely<import('../sql.generated').Database>} dbClient
 * @returns {import('../types').ContentTable}
 */
export function useContentTable (dbClient) {
  return {
    insert: async (contentItem) => {
      const inserted = (new Date()).toISOString()
      const item = {
        link: `${contentItem.link}`,
        size: contentItem.size,
        bucket_name: contentItem.bucketName,
        bucket_endpoint: contentItem.bucketEndpoint,
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
    }
  }
}
