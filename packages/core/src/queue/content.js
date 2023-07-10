import { parse as parseLink } from 'multiformats/link'

import { connect } from '../database/index.js'
import { DEFAULT_LIMIT } from '../database/constants.js'
import { DatabaseOperationError } from '../database/errors.js'

export const CONTENT = 'content'
export const CONTENT_QUEUE = 'content_queue'

/**
 * @typedef {import('../types').ContentSource} ContentSource
 */

/**
 * @param {import('../types.js').DatabaseConnect} conf
 * @returns {import('../types.js').ContentQueue}
 */
export function createContentQueue (conf) {
  const dbClient = connect(conf)

  return {
    put: async (contentItem) => {
      const item = {
        link: `${contentItem.link}`,
        size: contentItem.size,
        source: JSON.stringify(contentItem.source),
      }

      try {
        await dbClient
          .insertInto(CONTENT)
          .values(item)
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
    peek: async ({ limit = DEFAULT_LIMIT } = {}) => {
      let queuePeakResponse
      try {
        queuePeakResponse = await dbClient
          .selectFrom(CONTENT_QUEUE)
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
        link: parseLink(/** @type {string} */ (content.link)),
        size: /** @type {number} */(Number.parseInt(/** @type {string} */ (content.size))) | 0,
        source: /** @type {ContentSource[]} */ (content.source),
        inserted: /** @type {Date} */(content.inserted).toISOString(),
      }))

      return {
        ok: contentQueue
      }
    }
  }
}
