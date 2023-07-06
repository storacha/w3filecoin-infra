import anyTest from 'ava'
import dotenv from 'dotenv'

dotenv.config({
  path: '.env.local'
})

/**
 * @typedef {object} Context
 * @property {string} apiEndpoint
 *
 * @typedef {import('ava').TestFn<Awaited<Context>>} TestContextFn
 */

// eslint-disable-next-line unicorn/prefer-export-from
export const test  = /** @type {TestContextFn} */ (anyTest)
