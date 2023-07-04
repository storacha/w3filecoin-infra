import { Kysely } from 'kysely'
import { parse as parseLink } from 'multiformats/link'

import { getDialect } from '../table/utils.js'
import {
  DEFAULT_LIMIT,
} from '../table/constants.js'
import {
  DatabaseOperationError,
} from '../table/errors.js'

export const VIEW_NAME = 'cargo'

/**
 * @param {import('../types').DialectProps} dialectOpts
 */
export function createCargoView (dialectOpts) {
  const dialect = getDialect(dialectOpts)

  const dbClient = new Kysely({
    dialect
  })

  return useCargoView(dbClient)
}

/**
 * 
 * @param {import('kysely').Kysely<import('../sql.generated').Database>} dbClient
 * @returns {import('../types').CargoView}
 */
export function useCargoView (dbClient) {
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

      /** @type {import('../types').CargoOutput[]} */
      // @ts-expect-error sql created types for view get optional
      // while in practise they will always have a value
      const cargo = res.map(content => ({
        piece: content.piece !== null && parseLink(content.piece),
        priority: content.priority,
        inserted: content.inserted,
      }))

      return {
        ok: cargo
      }
    }
  }
}
