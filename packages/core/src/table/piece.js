import { Kysely } from 'kysely'

import { useInclusionTable } from './inclusion.js'
import { getDialect } from './utils.js'
import {
  SQLSTATE_UNIQUE_VALUE_CONSTRAINT
} from './constants.js'
import {
  DatabaseOperationError,
  DatabaseUniqueValueConstraintError
} from './errors.js'

export const TABLE_NAME = 'piece'

/**
 * @param {import('../types').DialectProps} dialectOpts
 */
export function createPieceTable (dialectOpts) {
  const dialect = getDialect(dialectOpts)

  const dbClient = new Kysely({
    dialect
  })

  return usePieceTable(dbClient)
}

/**
 * 
 * @param {import('kysely').Kysely<import('../sql.generated').Database>} dbClient
 * @returns {import('../types').PieceTable}
 */
export function usePieceTable (dbClient) {
  return {
    insert: async (pieceItem, contentLink) => {
      const inserted = (new Date()).toISOString()
      const item = {
        link: `${pieceItem.link}`,
        size: pieceItem.size,
        inserted,
        content: `${contentLink}`,
      }

      try {
        // Transaction
        await dbClient.transaction().execute(async trx => {
          // create include table backed by transaction client
          const inclusionTable = useInclusionTable(trx)

          // Insert to piece table
          await trx
            .insertInto(TABLE_NAME)
            .values(item)
            .execute()

          const { error } = await inclusionTable.insert({
            piece: pieceItem.link
          })
          if (error) {
            throw error
          }
        })
      } catch (/** @type {any} */ error) {
        return {
          error: Number.parseInt(error.code) === SQLSTATE_UNIQUE_VALUE_CONSTRAINT ?
            new DatabaseUniqueValueConstraintError(error.message) :
            new DatabaseOperationError(error.message)
        }
      }

      return {
        ok: {}
      }
    }
  }
}
