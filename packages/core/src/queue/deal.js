import { parse as parseLink } from 'multiformats/link'

import { connect } from '../database/index.js'
import {
  DEFAULT_LIMIT
} from '../database/constants.js'
import {
  DatabaseOperationError,
} from '../database/errors.js'

export const TABLE_NAME = 'deal'
export const VIEW_NAME = 'deal_signed'
/**
 * @type {Record<string, import('../schema').DealStatus>}
 */
export const STATUS = {
  APPROVED: 'APPROVED',
  PENDING: 'PENDING',
  REJECTED: 'REJECTED',
  SIGNED: 'SIGNED'
}

/**
 * 
 * @param {import('../types.js').DatabaseConnect} conf
 * @returns {import('../types.js').DealQueue}
 */
export function createDealQueue (conf) {
  const dbClient = connect(conf)

  return {
    put: async (dealItems) => {
      const items = dealItems.map(item => ({
        aggregate: `${item.aggregate}`,
        status: STATUS.PENDING,
      }))

      try {
        await dbClient
          .insertInto(TABLE_NAME)
          .values(items)
          // NOOP if item is already in table
          .onConflict(oc => oc
            .column('aggregate')
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

      /** @type {import('../types.js').Inserted<import('../types').Deal>[]} */
      const deals = queuePeakResponse.map(d => ({
        // @ts-expect-error sql created types for view get optional
        aggregate: parseLink(/** @type {string} */ d.aggregate),
        inserted: /** @type {Date} */(d.inserted).toISOString(),
      }))

      return {
        ok: deals
      }
    }
  }
}
