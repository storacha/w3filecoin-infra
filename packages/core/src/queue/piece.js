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

export const TABLE_NAME = 'piece'
export const INCLUSION_TABLE_NAME = 'inclusion'
export const VIEW_NAME = 'cargo'

/**
 * @param {import('../types.js').DialectProps} dialectOpts
 */
export function createPieceQueue (dialectOpts) {
  const dialect = getDialect(dialectOpts)
  const dbClient = new Kysely({
    dialect
  })

  return usePieceQueue(dbClient)
}

/**
 * 
 * @param {import('kysely').Kysely<import('../schema.js').Database>} dbClient
 * @returns {import('../types.js').PieceQueue}
 */
export function usePieceQueue (dbClient) {
  return {
    put: async (pieceItems) => {
      const items = pieceItems.map(pieceItem => ({
        link: `${pieceItem.link}`,
        size: pieceItem.size,
        content: `${pieceItem.content}`,
      }))

      try {
        // Transaction
        await dbClient.transaction().execute(async trx => {
          // Insert to piece table
          await trx
            .insertInto(TABLE_NAME)
            .values(items)
            .execute()

          // Insert to inclusion table all pieces
          await trx
            .insertInto(INCLUSION_TABLE_NAME)
            .values(items.map(item => ({
              piece: item.link,
              priority: 0
            })))
            .execute()
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

      /** @type {import('../types.js').Inserted<import('../types').Inclusion>[]} */
      const cargo = queuePeakResponse.map(piece => ({
        // @ts-expect-error sql created types for view get optional
        piece: parseLink(/** @type {string} */ piece.piece),
        priority: /** @type {number} */(piece.priority),
        inserted: /** @type {Date} */(piece.inserted).toISOString(),
      }))

      return await consumer(cargo)
    }
  }
}
