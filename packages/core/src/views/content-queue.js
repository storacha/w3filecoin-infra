import { Kysely } from 'kysely'
import { parse as parseLink } from 'multiformats/link'

import { getDialect } from '../table/utils.js'
import {
  DEFAULT_LIMIT,
} from '../table/constants.js'
import {
  DatabaseOperationError,
} from '../table/errors.js'

export const VIEW_NAME = 'content_queue'

/**
 * @param {import('../types').DialectProps} dialectOpts
 */
export function createContentQueueView (dialectOpts) {
  const dialect = getDialect(dialectOpts)

  const dbClient = new Kysely({
    dialect
  })

  return useContentQueueView(dbClient)
}

/**
 * 
 * @param {import('kysely').Kysely<import('../schema').Database>} dbClient
 * @returns {import('../types').ContentQueueView}
 */
export function useContentQueueView (dbClient) {
  return {
    selectAll: async (options = {}) => {
      const limit = options.limit || DEFAULT_LIMIT

      let res
      try {
        res = await dbClient
          .selectFrom(VIEW_NAME)
          .selectAll()
          .limit(limit)
          .execute()
      } catch (/** @type {any} */ error) {
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      /** @type {import('../types').ContentOutput[]} */
      const contentQueue = res.map(content => ({
        // @ts-expect-error sql created types for view get optional
        link: parseLink(/** @type {string} */ content.link),
        // @ts-expect-error sql created types for view get optional
        size: /** @type {number} */(Number.parseInt(content.size)) || 0,
        // @ts-expect-error sql created types for view get optional
        source: /** @type {ContentSource[]} */ (content.source),
        inserted: /** @type {Date} */(content.inserted).toISOString(),
      }))

      return {
        ok: contentQueue
      }
    }
  }
}
