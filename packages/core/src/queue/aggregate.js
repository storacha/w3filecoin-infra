import { Kysely } from 'kysely'

import { getDialect } from '../table/utils.js'
import {
  SQLSTATE_UNIQUE_VALUE_CONSTRAINT_ERROR_CODE,
  DEFAULT_LIMIT
} from '../table/constants.js'
import {
  DatabaseOperationError,
  DatabaseUniqueValueConstraintError
} from '../table/errors.js'

export const TABLE_NAME = 'aggregate'
export const INCLUSION_TABLE_NAME = 'inclusion'
export const VIEW_NAME = 'aggregate_queue'

/**
 * @param {import('../types.js').DialectProps} dialectOpts
 */
export function createAggregateQueue (dialectOpts) {
  const dialect = getDialect(dialectOpts)
  const dbClient = new Kysely({
    dialect
  })

  return useAggregateQueue(dbClient)
}

/**
 * 
 * @param {import('kysely').Kysely<import('../schema.js').Database>} dbClient
 * @returns {import('../types.js').AggregateQueue}
 */
export function useAggregateQueue (dbClient) {
  return {
    put: async (aggregateItems) => {
      const items = aggregateItems.map(aggregateItem => ({
        link: `${aggregateItem.link}`,
        size: aggregateItem.size,
        pieces: aggregateItem.pieces
      }))

      try {
        // Transaction
        await dbClient.transaction().execute(async trx => {
          // Insert to aggregate table
          await trx
            .insertInto(TABLE_NAME)
            .values(items)
            .execute()

          // Update inclusion table to point to aggregates
          await Promise.all(items.map(item => trx
            .updateTable(INCLUSION_TABLE_NAME)
            .set({
              aggregate: item.link.toString()
            })
            .where('aggregate', 'is', null)
            .where('piece', 'in', item.pieces.map(i => i.toString()))
            .execute()
          ))
        })
      } catch (/** @type {any} */ error) {
        return {
          error: Number.parseInt(error.code) === SQLSTATE_UNIQUE_VALUE_CONSTRAINT_ERROR_CODE ?
            new DatabaseUniqueValueConstraintError(error.message) :
            new DatabaseOperationError(error.message)
        }
      }

      return {
        ok: {}
      }
    },
    consume: async (consumer, options = {}) => {
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

      /** @type {import('../types.js').Inserted<import('../types').Aggregate>[]} */
      const aggregateQueue = queuePeakResponse.map(aggregate => ({
        // @ts-expect-error sql created types for view get optional
        link: parseLink(/** @type {string} */ aggregate.link),
        // @ts-expect-error sql created types for view get optional
        size: /** @type {number} */(Number.parseInt(aggregate.size)) || 0,
        inserted: /** @type {Date} */(aggregate.inserted).toISOString(),
      }))

      return await consumer(aggregateQueue)
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

      /** @type {import('../types.js').Inserted<import('../types').Aggregate>[]} */
      const aggregateQueue = queuePeakResponse.map(aggregate => ({
        // @ts-expect-error sql created types for view get optional
        link: parseLink(/** @type {string} */ aggregate.link),
        // @ts-expect-error sql created types for view get optional
        size: /** @type {number} */(Number.parseInt(aggregate.size)) || 0,
        inserted: /** @type {Date} */(aggregate.inserted).toISOString(),
      }))

      return {
        ok: aggregateQueue
      }
    }
  }
}
