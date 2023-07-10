import { parse as parseLink } from 'multiformats/link'

import { connect } from '../database/index.js'
import {
  DEFAULT_LIMIT
} from '../database/constants.js'
import {
  DatabaseOperationError,
} from '../database/errors.js'

export const DEAL = 'deal'
export const DEAL_SIGNED = 'deal_signed'
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
          .insertInto(DEAL)
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
    peek: async ({ limit = DEFAULT_LIMIT } = {}) => {
      let queuePeakResponse
      try {
        queuePeakResponse = await dbClient
          .selectFrom(DEAL_SIGNED)
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
        aggregate: parseLink(/** @type {string} */ (d.aggregate)),
        inserted: /** @type {Date} */(d.inserted).toISOString(),
      }))

      return {
        ok: deals
      }
    }
  }
}
