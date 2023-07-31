import anyTest from 'ava'

/**
 * @typedef {object} QueueContext
 * @property {import('@aws-sdk/client-sqs').SQSClient} sqsClient
 * @property {string} queueName
 * @property {string} queueUrl
 * @property {import('sqs-consumer').Consumer} queueConsumer
 * @property {import('@aws-sdk/client-sqs').Message[]} queueMessages
 * 
 * @typedef {object} DbContext
 * @property {import('@aws-sdk/client-dynamodb').DynamoDBClient} dynamoClient
 * @property {string} dynamoEndpoint
 * 
 * @typedef {import('ava').TestFn<any>} Test
 * @typedef {import('ava').TestFn<QueueContext & DbContext>} TestService
 */

// eslint-disable-next-line unicorn/prefer-export-from
export const test = /** @type {Test} */ (anyTest)

// eslint-disable-next-line unicorn/prefer-export-from
export const testService = /** @type {TestService} */ (anyTest)
