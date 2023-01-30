import anyTest from 'ava'

/**
 * @typedef {object} Context
 * @property {string} dbEndpoint
 * @property {import('@aws-sdk/client-dynamodb').DynamoDBClient} dynamoClient
 * @property {string} redisEndpoint
 * @property {number} redisPort
 *
 * @typedef {object} AggregateContext
 * @property {string} dbEndpoint
 * @property {import('@aws-sdk/client-dynamodb').DynamoDBClient} dynamoClient
 *
 * @typedef {import("ava").TestFn<Awaited<Context>>} TestFn
 * @typedef {import("ava").TestFn<Awaited<AggregateContext>>} TestAggregateFn
 */

// eslint-disable-next-line unicorn/prefer-export-from
export const test  = /** @type {TestFn} */ (anyTest)

// eslint-disable-next-line unicorn/prefer-export-from
export const testAggregate  = /** @type {TestAggregateFn} */ (anyTest)
