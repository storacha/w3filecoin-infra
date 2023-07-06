import { Kysely } from 'kysely'

import { getDialect } from '../table/utils.js'
import {
  SQLSTATE_UNIQUE_VALUE_CONSTRAINT_ERROR_CODE,
  DEFAULT_LIMIT
} from '../table/constants.js'
import {
  DatabaseOperationError,
  DatabaseUniqueValueConstraintError
} from '../table/errors.js'

export const TABLE_NAME = 'content'
export const VIEW_NAME = 'content_queue'

/**
 * @param {import('../types.js').DialectProps} dialectOpts
 */
export function createContentQueue (dialectOpts) {
  const dialect = getDialect(dialectOpts)
  const dbClient = new Kysely({
    dialect
  })

  return useContentQueue(dbClient)
}

/**
 * 
 * @param {import('kysely').Kysely<import('../schema.js').Database>} dbClient
 * @returns {import('../types.js').ContentQueue}
 */
export function useContentQueue (dbClient) {
  return {
    put: async (contentItems) => {
      const items = contentItems.map(contentItem => ({
        link: `${contentItem.link}`,
        size: contentItem.size,
        source: JSON.stringify(contentItem.source),
      }))

      try {
        await dbClient
          .insertInto(TABLE_NAME)
          .values(items)
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
    },
    consume: async (consumer, options = {}) => {
      const limit = options.limit || DEFAULT_LIMIT

      let queuePeakResponse
      try {
        queuePeakResponse = await dbClient
          .selectFrom(VIEW_NAME)
          .selectAll()
          .limit(limit)
          .execute()
      } catch (/** @type {any} */ error) {
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      /** @type {import('../types.js').Inserted<import('../types').Content>[]} */
      const contentQueue = queuePeakResponse.map(content => ({
        // @ts-expect-error sql created types for view get optional
        link: parseLink(/** @type {string} */ content.link),
        // @ts-expect-error sql created types for view get optional
        size: /** @type {number} */(Number.parseInt(content.size)) || 0,
        // @ts-expect-error sql created types for view get optional
        source: /** @type {ContentSource[]} */ (content.source),
        inserted: /** @type {Date} */(content.inserted).toISOString(),
      }))

      return await consumer(contentQueue)
    }
  }
}
