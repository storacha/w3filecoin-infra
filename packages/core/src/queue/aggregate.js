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

export const TABLE_NAME = 'aggregate'
export const INCLUSION_TABLE_NAME = 'inclusion'
export const VIEW_NAME = 'aggregate_queue'

/**
 * 
 * @param {import('../types.js').DatabaseConnect} conf
 * @returns {import('../types.js').AggregateQueue}
 */
export function createAggregateQueue (conf) {
  const dbClient = connect(conf)

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
          const insertOps = await trx
            .insertInto(TABLE_NAME)
            .values(items.map(item => ({
              link: item.link,
              size: item.size
            })))
            // NOOP if item is already in table
            .onConflict(oc => oc
              .column('link')
              .doNothing()
            )
            .execute()

          // Update inclusion table to point to aggregates
          const updateOpsPerAggregate = await Promise.all(items.map(item => trx
            .updateTable(INCLUSION_TABLE_NAME)
            .set({
              aggregate: item.link.toString()
            })
            .where('aggregate', 'is', null)
            .where('piece', 'in', item.pieces.map(i => i.toString()))
            .execute()
          ))

          // Verify if all cargo items for each aggregates were updated.
          // This enables the producer to receive a batch of aggregates to add to the
          // queue where one aggregate is already there, while making sure
          // the inserted ones had their pieces fully included.
          for (let i = 0; i < updateOpsPerAggregate.length; i++) {
            // Get number of inclusion rows updated per received aggregate
            const updatedItemsCount = updateOpsPerAggregate[i].reduce((acc, b) => {
              return acc + Number(b.numUpdatedRows)
            }, 0)

            // If aggregate was inserted, but items in its inclusion table were
            // not fully updated, than we need to rollback.
            // Where clause guarantees that only changes when aggregate is null
            if (
              Number(insertOps[i].numInsertedOrUpdatedRows) === 1
              && updatedItemsCount !== items[i].pieces.length
            ) {
              throw new DatabaseValueToUpdateAlreadyTakenError()
            }
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
