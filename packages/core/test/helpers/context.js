import anyTest from 'ava'

/**
 * @typedef {object} QueueContext
 * @property {string} queueName
 * @property {string} queueUrl
 * @property {import('sqs-consumer').Consumer} queueConsumer
 *
 * @typedef {object} DbContext
 * @property {import('@aws-sdk/client-dynamodb').DynamoDBClient} dynamoClient
 * @property {string} dynamoEndpoint
 *
 * @typedef {object} BucketContext
 * @property {import('@aws-sdk/client-s3').S3Client} s3
 *
 * @typedef {object} MultipleQueueContext
 * @property {import('@aws-sdk/client-sqs').SQSClient} sqsClient
 * @property {Record<string, QueueContext>} queues
 * @property {Map<string, unknown[]>} queuedMessages
 *
 * @typedef {object} Stoppable
 * @property {() => Promise<any>} stop
 *
 * @typedef {import('ava').TestFn<any>} Test
 * @typedef {import('ava').TestFn<QueueContext & DbContext & Stoppable>} TestService
 * @typedef {import('ava').TestFn<BucketContext & DbContext & Stoppable>} TestStore
 * @typedef {import('ava').TestFn<QueueContext>} TestQueue
 * @typedef {import('ava').TestFn<BucketContext & DbContext & QueueContext>} TestWorkflow
 * @typedef {import('ava').TestFn<BucketContext & DbContext>} TestDealTracker
 * @typedef {import('ava').TestFn<BucketContext & DbContext & MultipleQueueContext & Stoppable>} TestWorkflowWithMultipleQueues
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

// eslint-disable-next-line unicorn/prefer-export-from
export const testDealTracker = /** @type {TestDealTracker} */ (anyTest)

// eslint-disable-next-line unicorn/prefer-export-from
export const tesWorkflowWithMultipleQueues = /** @type {TestWorkflowWithMultipleQueues} */ (anyTest)
