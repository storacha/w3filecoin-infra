import { parse as parseLink } from 'multiformats/link'
import { sql } from 'kysely'

import { connect } from '../database/index.js'
import {
  SQLSTATE_FOREIGN_KEY_CONSTRAINT_ERROR_CODE,
  DEFAULT_LIMIT
} from '../database/constants.js'
import {
  DatabaseOperationError,
  DatabaseForeignKeyConstraintError
} from '../database/errors.js'

export const PIECE = 'piece'
export const INCLUSION = 'inclusion'
export const CARGO = 'cargo'

/**
 * @param {import('../types').Piece} pieceItem 
 */
const encode = (pieceItem) => ({
  link: `${pieceItem.link}`,
  size: pieceItem.size,
  content: `${pieceItem.content}`,
  priority: pieceItem.priority || 0
})

/**
 * @param {any[]} rows 
 * @returns {import('../types.js').Inserted<import('../types').Inclusion>[]}
 */
const decode = (rows) => {
  return rows.map(piece => ({
    piece: parseLink(/** @type {string} */ (piece.piece)),
    priority: /** @type {number} */(piece.priority),
    inserted: /** @type {Date} */(piece.inserted).toISOString(),
    aggregate: piece.aggregate && parseLink(/** @type {string} */ piece.aggregate),
    size: /** @type {bigint} */(BigInt(/** @type {string} */ (piece.size))) | 0n,
  }))
}

/**
 * 
 * @param {import('../types.js').DatabaseConnect} conf
 * @returns {import('../types.js').PieceQueue}
 */
export function createPieceQueue (conf) {
  const dbClient = connect(conf)

  return {
    put: async (pieceItem) => {
      const item = encode(pieceItem)

      try {
        // Transaction
        await dbClient.transaction().execute(async trx => {
          // Insert to piece table
          await trx
            .insertInto(PIECE)
            .values({
              link: item.link,
              size: item.size,
              content: item.content
            })
            // NOOP if item is already in table
            .onConflict(oc => oc
              .column('link')
              .doNothing()
            )
            .execute()

          // Insert to inclusion table all pieces
          await trx
            .insertInto(INCLUSION)
            .values({
              piece: item.link,
              priority: item.priority,
              aggregate: null
            })
            // NOOP if item is already in table
            .onConflict(oc => oc
              .expression(sql`piece, COALESCE(aggregate, '')`)
              .doNothing()
            )
            .execute()
        })
      } catch (/** @type {any} */ error) {
        return {
          error: Number.parseInt(error.code) === SQLSTATE_FOREIGN_KEY_CONSTRAINT_ERROR_CODE ?
            new DatabaseForeignKeyConstraintError(error.message) :
            new DatabaseOperationError(error.message)
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
          .selectFrom(CARGO)
          .selectAll()
          .limit(limit)
          .offset(offset)
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
