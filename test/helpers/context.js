import anyTest from 'ava'
import dotenv from 'dotenv'

dotenv.config({
  path: '.env.local'
})

/**
 * @typedef {object} Dynamo
 * @property {import('@aws-sdk/client-dynamodb').DynamoDBClient} client
 * @property {string} endpoint
 * @property {string} region
 * @property {string} tableName
 * 
 * @typedef {object} Bucket
 * @property {import('@aws-sdk/client-s3').S3Client} client
 * @property {string} region
 * @property {string} bucket
 *
 * @typedef {object} Context
 * @property {string} apiEndpoint
 *
 * @typedef {import('ava').TestFn<Awaited<Context>>} TestContextFn
 */

// eslint-disable-next-line unicorn/prefer-export-from
export const test  = /** @type {TestContextFn} */ (anyTest)
