import { testQueue as test } from './helpers/context.js'
import { createQueue } from './helpers/resources.js'
import { randomCargo, randomAggregate } from './helpers/cargo.js'

import { Consumer } from 'sqs-consumer'
import pWaitFor from 'p-wait-for'
import delay from 'delay'
import { EncodeRecordErrorName } from '@web3-storage/filecoin-api-legacy/errors'

import { encode as pieceEncode, decode as pieceDecode } from '../src/data/piece.js'
import { encode as bufferEncode, decode as bufferDecode } from '../src/data/buffer.js'
import { encode as aggregateEncode, decode as aggregateDecode } from '../src/data/aggregate.js'

import { createQueueClient } from '../src/queue/client.js'

test.beforeEach(async (t) => {
  const sqs = await createQueue()

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

test('can queue received pieces', async t => {
  const { sqsClient, queueUrl, queuedMessages } = t.context
  const [cargo] = await randomCargo(1, 128)

  const queueClient = createQueueClient(sqsClient, {
    queueUrl,
    encodeMessage: pieceEncode.message,
  })

  const piece = cargo.link
  const storefront = 'did:web:web3.storage'
  const group = 'did:web:free.web3.storage'
  const pieceRow = {
    piece,
    storefront,
    group,
    insertedAt: Date.now()
  }

  // add to queue
  const addRes = await queueClient.add(pieceRow)
  t.truthy(addRes.ok)

  // Validate message received to queue
  await pWaitFor(() => queuedMessages.length === 1)

  const pieceRef = await pieceDecode.message(queuedMessages[0].Body || '')
  t.truthy(pieceRef)
})

// TODO: error encode
test('fails to queue when failing to encode', async t => {
  const { sqsClient, queueUrl, queuedMessages } = t.context
  const [cargo] = await randomCargo(1, 128)

  const queueClient = createQueueClient(sqsClient, {
    queueUrl,
    encodeMessage: async () => {
      throw new Error('failed to encode')
    },
  })

  const piece = cargo.link
  const storefront = 'did:web:web3.storage'
  const group = 'did:web:free.web3.storage'
  const pieceRow = {
    piece,
    storefront,
    group,
    insertedAt: Date.now()
  }

  // add to queue
  const addRes = await queueClient.add(pieceRow)
  t.falsy(addRes.ok)
  t.truthy(addRes.error)
  t.is(addRes.error?.name, EncodeRecordErrorName)

  // Validate message not received
  await pWaitFor(() => queuedMessages.length === 0)
})

test('can queue received buffers', async t => {
  const { sqsClient, queueUrl, queuedMessages } = t.context
  const pieces = await randomCargo(1, 128)
  const storefront = 'did:web:web3.storage'
  const group = 'did:web:free.web3.storage'

  const queueClient = createQueueClient(sqsClient, {
    queueUrl,
    encodeMessage: bufferEncode.message,
  })

  const bufferedPieces = pieces.map(p => ({
    piece: p.link,
    insertedAt: Date.now(),
    policy: /** @type {import('../src/data/types.js').PiecePolicy} */ (0),
  }))
  const buffer = {
    pieces: bufferedPieces,
    storefront,
    group,
  }

  // add to queue
  const addRes = await queueClient.add(buffer)
  t.truthy(addRes.ok)

  // Validate message received to queue
  await pWaitFor(() => queuedMessages.length === 1)
  const bufferRef = await bufferDecode.message(queuedMessages[0].Body || '')
  t.truthy(bufferRef)
})

test('can queue received aggregates', async t => {
  const { sqsClient, queueUrl, queuedMessages } = t.context
  const { aggregate, pieces } = await randomAggregate(10, 128)
  const storefront = 'did:web:web3.storage'
  const group = 'did:web:free.web3.storage'

  const queueClient = createQueueClient(sqsClient, {
    queueUrl,
    encodeMessage: aggregateEncode.message,
  })

  const aggregateRow = {
    piece: aggregate.link,
    buffer: pieces[0].content, // random CID for testing
    invocation: pieces[0].content, // random CID for testing
    task: pieces[0].content, // random CID for testing
    insertedAt: Date.now(),
    storefront,
    group,
    stat: /** @type {import('../src/data/types.js').AggregateStatus} */ (0),
  }

  // add to queue
  const addRes = await queueClient.add(aggregateRow)
  t.truthy(addRes.ok)

  // Validate message received to queue
  await pWaitFor(() => queuedMessages.length === 1)
  const aggregateRef = await aggregateDecode.message(queuedMessages[0].Body || '')
  t.truthy(aggregateRef)
})
