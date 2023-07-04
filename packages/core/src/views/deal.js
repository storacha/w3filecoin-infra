import { Kysely } from 'kysely'
import { parse as parseLink } from 'multiformats/link'

import { getDialect } from '../table/utils.js'
import {
  DEFAULT_LIMIT,
} from '../table/constants.js'
import {
  DatabaseOperationError,
} from '../table/errors.js'

export const PENDING_VIEW_NAME = 'deal_pending'
export const SIGNED_VIEW_NAME = 'deal_signed'
export const APPROVED_VIEW_NAME = 'deal_approved'
export const REJECTED_VIEW_NAME = 'deal_rejected'

/**
 * @param {import('../types').DialectProps} dialectOpts
 */
export function createDealView (dialectOpts) {
  const dialect = getDialect(dialectOpts)

  const dbClient = new Kysely({
    dialect
  })

  return useDealView(dbClient)
}

/**
 * 
 * @param {import('kysely').Kysely<import('../sql.generated').Database>} dbClient
 * @returns {import('../types').DealView}
 */
export function useDealView (dbClient) {
  return {
    selectPending: async (options = {}) => {
      const limit = options.limit || DEFAULT_LIMIT

      let res
      try {
        res = await dbClient
          .selectFrom(PENDING_VIEW_NAME)
          .selectAll()
          .limit(limit)
          .execute()
      } catch (/** @type {any} */ error) {
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      /** @type {import('../types').DealPendingOutput[]} */
      // @ts-expect-error sql created types for view get optional
      // while in practise they will always have a value
      const deals = res.map(d => ({
        aggregate: d.aggregate !== null && parseLink(d.aggregate),
        inserted: d.inserted,
      }))

      return {
        ok: deals
      }
    },
    selectSigned: async (options = {}) => {
      const limit = options.limit || DEFAULT_LIMIT

      let res
      try {
        res = await dbClient
          .selectFrom(SIGNED_VIEW_NAME)
          .selectAll()
          .limit(limit)
          .execute()
      } catch (/** @type {any} */ error) {
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      /** @type {import('../types').DealSignedOutput[]} */
      // @ts-expect-error sql created types for view get optional
      // while in practise they will always have a value
      const deals = res.map(d => ({
        aggregate: d.aggregate !== null && parseLink(d.aggregate),
        signed: d.signed,
      }))

      return {
        ok: deals
      }
    },
    selectApproved: async (options = {}) => {
      const limit = options.limit || DEFAULT_LIMIT

      let res
      try {
        res = await dbClient
          .selectFrom(APPROVED_VIEW_NAME)
          .selectAll()
          .limit(limit)
          .execute()
      } catch (/** @type {any} */ error) {
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      /** @type {import('../types').DealProcessedOutput[]} */
      // @ts-expect-error sql created types for view get optional
      // while in practise they will always have a value
      const deals = res.map(d => ({
        aggregate: d.aggregate !== null && parseLink(d.aggregate),
        processed: d.processed,
      }))

      return {
        ok: deals
      }
    },
    selectRejected: async (options = {}) => {
      const limit = options.limit || DEFAULT_LIMIT

      let res
      try {
        res = await dbClient
          .selectFrom(REJECTED_VIEW_NAME)
          .selectAll()
          .limit(limit)
          .execute()
      } catch (/** @type {any} */ error) {
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      /** @type {import('../types').DealProcessedOutput[]} */
      // @ts-expect-error sql created types for view get optional
      // while in practise they will always have a value
      const deals = res.map(d => ({
        aggregate: d.aggregate !== null && parseLink(d.aggregate),
        processed: d.processed,
      }))

      return {
        ok: deals
      }
    }
  }
}
