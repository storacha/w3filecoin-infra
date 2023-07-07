import { parse as parseLink } from 'multiformats/link'

import { connect } from '../database/index.js'
import { DEFAULT_LIMIT } from '../database/constants.js'
import { DatabaseOperationError } from '../database/errors.js'

export const TABLE_NAME = 'content'
export const VIEW_NAME = 'content_queue'

/**
 * @param {import('../types.js').DatabaseConnect} conf
 * @returns {import('../types.js').ContentQueue}
 */
export function createContentQueue (conf) {
  const dbClient = connect(conf)

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
          // NOOP if item is already in queue
          .onConflict(oc => oc
            .column('link')
            .doNothing()
          )
          .execute()
      } catch (/** @type {any} */ error) {
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      return {
        ok: {}
      }
    },
    peek: async (options = {}) => {
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

      return {
        ok: contentQueue
      }
    }
  }
}
