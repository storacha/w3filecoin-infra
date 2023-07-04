import { Kysely } from 'kysely'
import { parse as parseLink } from 'multiformats/link'

import { getDialect } from '../table/utils.js'
import {
  DEFAULT_LIMIT,
} from '../table/constants.js'
import {
  DatabaseOperationError,
} from '../table/errors.js'

export const VIEW_NAME = 'aggregate_queue'

/**
 * @param {import('../types').DialectProps} dialectOpts
 */
export function createAggregateQueueView (dialectOpts) {
  const dialect = getDialect(dialectOpts)

  const dbClient = new Kysely({
    dialect
  })

  return useAggregateQueueView(dbClient)
}

/**
 * 
 * @param {import('kysely').Kysely<import('../sql.generated').Database>} dbClient
 * @returns {import('../types').AggregateQueueView}
 */
export function useAggregateQueueView (dbClient) {
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

      /** @type {import('../types').AggregateOutput[]} */
      // @ts-expect-error sql created types for view get optional
      // while in practise they will always have a value
      const aggregagteQueue = res.map(aggregate => ({
        link: aggregate.link !== null && parseLink(aggregate.link),
        size: aggregate.size != null && Number.parseInt(aggregate.size),
        inserted: aggregate.inserted,
      }))

      return {
        ok: aggregagteQueue
      }
    }
  }
}
