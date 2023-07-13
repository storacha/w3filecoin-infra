import { parse as parseLink } from 'multiformats/link'

import { connect } from '../database/index.js'
import {
  SQLSTATE_UNIQUE_VALUE_CONSTRAINT_ERROR_CODE,
  DEFAULT_LIMIT
} from '../database/constants.js'
import {
  DatabaseOperationError,
  DatabaseValueToUpdateAlreadyTakenError,
  DatabaseValueToUpdateAlreadyTakenErrorName
} from '../database/errors.js'

export const AGGREGATE = 'aggregate'
export const INCLUSION = 'inclusion'
export const AGGREGATE_QUEUE = 'aggregate_queue'

/**
 * @param {import('../types').AggregateWithInclusionPieces} aggregateItem 
 */
const encode = (aggregateItem) => ({
  link: `${aggregateItem.link}`,
  size: aggregateItem.size,
  pieces: aggregateItem.pieces
})

/**
 * @param {any[]} rows 
 * @returns {import('../types.js').Inserted<import('../types').Aggregate>[]}
 */
const decode = (rows) => {
  return rows.map(aggregate => ({
    link: parseLink(/** @type {string} */ (aggregate.link)),
    size: /** @type {bigint} */(BigInt(/** @type {string} */ (aggregate.size))) | 0n,
    inserted: /** @type {Date} */(aggregate.inserted).toISOString(),
  }))
}

/**
 * 
 * @param {import('../types.js').DatabaseConnect} conf
 * @returns {import('../types.js').AggregateQueue}
 */
export function createAggregateQueue (conf) {
  const dbClient = connect(conf)

  return {
    put: async (aggregateItem) => {
      const item = encode(aggregateItem)

      try {
        // Transaction
        await dbClient.transaction().execute(async trx => {
          // Insert to aggregate table
          const insertOps = await trx
            .insertInto(AGGREGATE)
            .values({
              link: item.link,
              size: item.size
            })
            // NOOP if item is already in table
            .onConflict(oc => oc
              .column('link')
              .doNothing()
            )
            .execute()

          // Update inclusion table to point to aggregates
          const updatedOps = await trx
            .updateTable(INCLUSION)
            .set({
              aggregate: item.link.toString()
            })
            .where('aggregate', 'is', null)
            .where('piece', 'in', item.pieces.map(i => i.toString()))
            .execute()

          // Verify if all cargo items for the aggregate were updated.
          // If aggregate was inserted, but items in its inclusion table were
          // not fully updated, than we need to rollback.
          // Where clause guarantees that only changes when aggregate is null
          const updatedItems = updatedOps.reduce((acc, b) => {
            return acc + Number(b.numUpdatedRows)
          }, 0)
          if (Number(insertOps[0].numInsertedOrUpdatedRows) === 1
            && updatedItems !== item.pieces.length
            ) {
            throw new DatabaseValueToUpdateAlreadyTakenError()
          }
        })
      } catch (/** @type {any} */ error) {
        // If failing because item is already in the queue, return success
        if (Number.parseInt(error.code) === SQLSTATE_UNIQUE_VALUE_CONSTRAINT_ERROR_CODE) {
          return {
            ok: {}
          }
        } else if (error.name === DatabaseValueToUpdateAlreadyTakenErrorName) {
          return {
            error
          }
        }
        return {
          error: new DatabaseOperationError(error.message)
        }
      }

      return {
        ok: {}
      }
    },
    peek: async ({ limit = DEFAULT_LIMIT, offset = 0 } = {}) => {
      let queuePeakResponse
      try {
        queuePeakResponse = await dbClient
          .selectFrom(AGGREGATE_QUEUE)
          .selectAll()
          .offset(offset)
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
