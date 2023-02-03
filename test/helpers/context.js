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
 * @typedef {object} DataContext
 * @property {string} region
 * @property {Dynamo} aggregateDynamo
 * @property {Dynamo} carDynamo
 * @property {Dynamo} ferryDynamo
 * 
 * @typedef {import('ava').TestFn<Awaited<DataContext>>} TestDataFn
 * @typedef {import('ava').TestFn<Awaited<any>>} TestAnyFn
 */

// eslint-disable-next-line unicorn/prefer-export-from
export const test  = /** @type {TestAnyFn} */ (anyTest)

// eslint-disable-next-line unicorn/prefer-export-from
export const testData  = /** @type {TestDataFn} */ (anyTest)
