import { Aggregator } from '@web3-storage/filecoin-api/test'
import { ed25519 } from '@ucanto/principal'
import { Consumer } from 'sqs-consumer'
import pWaitFor from 'p-wait-for'

import { createQueueClient } from '../src/queue/client.js'
import { createTableStoreClient } from '../src/store/table-client.js'
import { pieceStoreTableProps } from '../src/store/index.js'
import { encode, decode } from '../src/data/piece.js'

import { testService as test } from './helpers/context.js'

import {
  createBucket,
  createDynamodDb,
  createTable,
  createQueue
} from './helpers/resources.js'

test.beforeEach(async (t) => {
  const sqs = await createQueue()
  const dynamo = await createDynamodDb()

  /** @type {import('@aws-sdk/client-sqs').Message[]} */
  const queueMessages = []
  const queueConsumer = Consumer.create({
    queueUrl: sqs.queueUrl,
    sqs: sqs.client,
    handleMessage: (message) => {
      queueMessages.push(message)
      return Promise.resolve()
    }
  })

  Object.assign(t.context, {
    dynamoClient: dynamo.client,
    sqsClient: sqs.client,
    queueName: sqs.queueName,
    queueUrl: sqs.queueUrl,
    queueConsumer,
    queueMessages
  })
})

test.beforeEach(async t => {
  t.context.queueConsumer.start()
  await pWaitFor(() => t.context.queueConsumer.isRunning)
})

test.afterEach(t => {
  t.context.queueConsumer.stop()
})

for (const [title, unit] of Object.entries(Aggregator.test)) {
  test(title, async (t) => {
    const { dynamoClient, sqsClient, queueUrl } = t.context
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

    const brokerDid = ''
    const brokerUrl = ''

    await unit(
      {
        equal: t.is,
        deepEqual: t.deepEqual,
        ok: t.true,
      },
      {
        id,
        errorReporter: {
          catch(error) {
            t.fail(error.message)
          },
        },
        pieceStore,
        addQueue,
        brokerDid,
        brokerUrl
      }
    )
  })
}
