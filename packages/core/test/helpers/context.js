import anyTest from 'ava'

/**
 * @typedef {object} QueueContext
 * @property {import('@aws-sdk/client-sqs').SQSClient} sqsClient
 * @property {string} queueName
 * @property {string} queueUrl
 * @property {import('sqs-consumer').Consumer} queueConsumer
 * @property {import('@aws-sdk/client-sqs').Message[]} queuedMessages
 * 
 * @typedef {object} DbContext
 * @property {import('@aws-sdk/client-dynamodb').DynamoDBClient} dynamoClient
 * @property {string} dynamoEndpoint
 * 
 * @typedef {object} BucketContext
 * @property {import('@aws-sdk/client-s3').S3Client} s3
 * 
 * @typedef {import('ava').TestFn<any>} Test
 * @typedef {import('ava').TestFn<QueueContext & DbContext>} TestService
 * @typedef {import('ava').TestFn<BucketContext & DbContext>} TestStore
 * @typedef {import('ava').TestFn<QueueContext>} TestQueue
 * @typedef {import('ava').TestFn<BucketContext & DbContext & QueueContext>} TestWorkflow
 */

// eslint-disable-next-line unicorn/prefer-export-from
export const test = /** @type {Test} */ (anyTest)

// eslint-disable-next-line unicorn/prefer-export-from
export const testStore = /** @type {TestStore} */ (anyTest)

// eslint-disable-next-line unicorn/prefer-export-from
export const testQueue = /** @type {TestQueue} */ (anyTest)

// eslint-disable-next-line unicorn/prefer-export-from
export const testService = /** @type {TestService} */ (anyTest)

// eslint-disable-next-line unicorn/prefer-export-from
export const tesWorkflow = /** @type {TestWorkflow} */ (anyTest)
