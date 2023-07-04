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
 * @param {import('kysely').Kysely<import('../sql.generated').Database>} dbClient
 * @returns {import('../types').ContentQueueView}
 */
export function useContentQueueView (dbClient) {
  return {
    select: async (options = {}) => {
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
      // @ts-expect-error sql created types for view get optional
      // while in practise they will always have a value
      const contentQueue = res.map(content => ({
        link: content.link !== null && parseLink(content.link),
        size: content.size != null && Number.parseInt(content.size),
        bucketName: content.bucket_name,
        bucketEndpoint: content.bucket_endpoint,
        inserted: content.inserted,
      }))

      return {
        ok: contentQueue
      }
    }
  }
}
