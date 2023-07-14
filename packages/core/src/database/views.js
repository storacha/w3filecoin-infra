import { parse as parseLink } from 'multiformats/link'

import { connect } from '../database/index.js'
import { DEFAULT_LIMIT } from './constants.js'
import { DatabaseOperationError } from './errors.js'

export const CARGO_INCLUDED = 'cargo_included'

/**
 * @param {import('../types.js').DatabaseConnect} conf
 * @returns {import('../types.js').DatabaseView}
 */
export function createView (conf) {
  const dbClient = connect(conf)

  return {
    selectCargoIncluded: async (aggregateLink, options = {}) => {
      const limit = options.limit || DEFAULT_LIMIT

      let res
      try {
        res = await dbClient
          .selectFrom(CARGO_INCLUDED)
          .selectAll()
          .where('aggregate', '=', aggregateLink.link().toString())
          .limit(limit)
          .execute()
      } catch (/** @type {any} */ error) {
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      return {
        ok: res.map(cargo => ({
          piece: parseLink(/** @type {string} */ (cargo.piece)),
          size: /** @type {bigint} */(BigInt(/** @type {string} */ (cargo.size))) | 0n,
        }))
      }
    }
  }
}
