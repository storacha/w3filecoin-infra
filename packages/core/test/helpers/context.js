import anyTest from 'ava'

/**
 * @typedef {import('../../src/sql.generated').Database} Database
 *
 * @typedef {object} DbContext
 * @property {import('kysely').Kysely<Database>} dbClient
 * 
 * @typedef {import('ava').TestFn<DbContext>} Test
 */

// eslint-disable-next-line unicorn/prefer-export-from
export const test = /** @type {Test} */ (anyTest)
