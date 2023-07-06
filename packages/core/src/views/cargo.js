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
 * @param {import('kysely').Kysely<import('../schema').Database>} dbClient
 * @returns {import('../types').CargoView}
 */
export function useCargoView (dbClient) {
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

      /** @type {import('../types').CargoOutput[]} */
      const cargo = res.map(content => ({
        // @ts-expect-error sql created types for view get optional
        piece: parseLink(/** @type {string} */ content.piece),
        priority: /** @type {number} */(content.priority),
        inserted: /** @type {Date} */(content.inserted).toISOString(),
      }))

      return {
        ok: cargo
      }
    }
  }
}
