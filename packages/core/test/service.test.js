import { Aggregator } from '@web3-storage/filecoin-api/test'
import { ed25519 } from '@ucanto/principal'
import { Consumer } from 'sqs-consumer'
import pWaitFor from 'p-wait-for'
import delay from 'delay'

import { createQueueClient } from '../src/queue/client.js'
import { createTableStoreClient } from '../src/store/table-client.js'
import { pieceStoreTableProps } from '../src/store/index.js'
import { encode, decode } from '../src/data/piece.js'

import { testService as test } from './helpers/context.js'

import {
  createDynamodDb,
  createTable,
  createQueue
} from './helpers/resources.js'

test.beforeEach(async (t) => {
  const sqs = await createQueue()
  const dynamo = await createDynamodDb()

  /** @type {import('@aws-sdk/client-sqs').Message[]} */
  const queuedMessages = []
  const queueConsumer = Consumer.create({
    queueUrl: sqs.queueUrl,
    sqs: sqs.client,
    handleMessage: (message) => {
      queuedMessages.push(message)
      return Promise.resolve()
    }
  })

  Object.assign(t.context, {
    dynamoClient: dynamo.client,
    sqsClient: sqs.client,
    queueName: sqs.queueName,
    queueUrl: sqs.queueUrl,
    queueConsumer,
    queuedMessages
  })
})

test.beforeEach(async t => {
  t.context.queueConsumer.start()
  await pWaitFor(() => t.context.queueConsumer.isRunning)
})

test.afterEach(async t => {
  t.context.queueConsumer.stop()
  await delay(1000)
})

for (const [title, unit] of Object.entries(Aggregator.test)) {
  const define = title.startsWith('only ')
    // eslint-disable-next-line no-only-tests/no-only-tests
    ? test.only
    : title.startsWith('skip ')
    ? test.skip
    : test

  define(title, async (t) => {
    const { dynamoClient, sqsClient, queueUrl, queuedMessages } = t.context
    const tableName = await createTable(dynamoClient, pieceStoreTableProps)

    // context
    const signer = await ed25519.generate()
    const id = signer.withDID('did:web:test.web3.storage')
    const pieceStore = createTableStoreClient(dynamoClient, {
      tableName,
      encodeRecord: encode.storeRecord,
      decodeRecord: decode.storeRecord,
      encodeKey: encode.storeKey
    })
    const addQueue = createQueueClient(sqsClient, {
      queueUrl,
      encodeMessage: encode.message,
    })
    await unit(
      {
        ok: (actual, message) => t.truthy(actual, message),
        equal: (actual, expect, message) =>
          t.is(actual, expect, message ? String(message) : undefined),
        deepEqual: (actual, expect, message) =>
          t.deepEqual(actual, expect, message ? String(message) : undefined),
      },
      {
        id,
        errorReporter: {
          catch(error) {
            t.fail(error.message)
          },
        },
        // @ts-expect-error needs https://github.com/web3-storage/w3up/pull/850
        pieceStore,
        // @ts-expect-error needs https://github.com/web3-storage/w3up/pull/850
        addQueue,
        queuedMessages
      }
    )
  })
}
