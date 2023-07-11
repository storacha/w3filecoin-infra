import anyTest from 'ava'

/**
 * @typedef {import('../../src/schema').Database} Database
 *
 * @typedef {object} DbContext
 * @property {import('kysely').Kysely<Database>} dbClient
 * 
 * @typedef {object} BucketContext
 * @property {import('@aws-sdk/client-s3').S3Client} s3Client
 * @property {string} bucketName
 * @property {any} s3ClientOpts
 * 
 * @typedef {object} QueueContext
 * @property {import('@aws-sdk/client-sqs').SQSClient} sqsClient
 * @property {string} queueName
 * @property {string} queueUrl
 * @property {import('sqs-consumer').Consumer} queueConsumer
 * @property {import('@aws-sdk/client-sqs').Message[]} queueMessages
 * 
 * @typedef {import('ava').TestFn<any>} Test
 * @typedef {import('ava').TestFn<BucketContext>} TestContentFetcher
 * @typedef {import('ava').TestFn<DbContext>} TestQueue
 * @typedef {import('ava').TestFn<DbContext & BucketContext & QueueContext>} TestWorkflow
 */

// eslint-disable-next-line unicorn/prefer-export-from
export const test = /** @type {Test} */ (anyTest)

// eslint-disable-next-line unicorn/prefer-export-from
export const testContentFetcher = /** @type {TestContentFetcher} */ (anyTest)

// eslint-disable-next-line unicorn/prefer-export-from
export const testQueue = /** @type {TestQueue} */ (anyTest)

// eslint-disable-next-line unicorn/prefer-export-from
export const testWorkflow = /** @type {TestWorkflow} */ (anyTest)
