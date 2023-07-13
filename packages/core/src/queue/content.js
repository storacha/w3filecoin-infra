import { parse as parseLink } from 'multiformats/link'

import { connect } from '../database/index.js'
import { DEFAULT_LIMIT } from '../database/constants.js'
import { DatabaseOperationError } from '../database/errors.js'

export const CONTENT = 'content'
export const CONTENT_QUEUE = 'content_queue'

/**
 * @param {import('../types').Content} contentItem 
 */
const encode = (contentItem) => ({
  link: `${contentItem.link}`,
  size: contentItem.size,
  source: JSON.stringify(contentItem.source),
})

/**
 * @param {any[]} rows 
 * @returns {import('../types.js').Inserted<import('../types').Content>[]}
 */
const decode = (rows) => {
  return rows.map(content => ({
    link: parseLink(/** @type {string} */ (content.link)),
    size: /** @type {number} */(Number.parseInt(/** @type {string} */ (content.size))) | 0,
    source: /** @type {URL[]} */ (content.source.map((/** @type {string} */ s) => new URL(s))),
    inserted: /** @type {Date} */(content.inserted).toISOString(),
  }))
}

/**
 * @param {import('../types.js').DatabaseConnect} conf
 * @returns {import('../types.js').ContentQueue}
 */
export function createContentQueue (conf) {
  const dbClient = connect(conf)

  return {
    put: async (contentItem) => {
      try {
        await dbClient
          .insertInto(CONTENT)
          .values(encode(contentItem))
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

      return {
        ok: decode(queuePeakResponse)
      }
    }
  }
}
