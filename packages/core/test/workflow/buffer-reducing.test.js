import { tesWorkflowWithMultipleQueues as test } from '../helpers/context.js'
import { createS3, createBucket, createQueue } from '../helpers/resources.js'
import { randomCargo } from '../helpers/cargo.js'

import { Consumer } from 'sqs-consumer'
import pWaitFor from 'p-wait-for'
import delay from 'delay'
import { QueueOperationFailed, StoreOperationErrorName, QueueOperationErrorName } from '@web3-storage/filecoin-api/errors'

import { encode as pieceEncode } from '../../src/data/piece.js'
import { encode as bufferEncode, decode as bufferDecode } from '../../src/data/buffer.js'
import { encode as aggregateEncode, decode as aggregateDecode } from '../../src/data/aggregate.js'
import { createBucketStoreClient } from '../../src/store/bucket-client.js'
import { createQueueClient } from '../../src/queue/client.js'

import { reduceBuffer } from '../../src/workflow/buffer-reducing.js'

/**
 * @typedef {import('../helpers/context.js').QueueContext} QueueContext
 * @typedef {import('../../src/data/types.js').PiecePolicy} PiecePolicy
 */

test.beforeEach(async (t) => {
  await delay(1000)
  const queueNames = ['buffer', 'aggregate']
  /** @type {Record<string, QueueContext>} */
  const queues = {}

  for (const name of queueNames) {
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

    queues[name] = {
      sqsClient: sqs.client,
      queueName: sqs.queueName,
      queueUrl: sqs.queueUrl,
      queueConsumer,
      queuedMessages
    }
  }

  Object.assign(t.context, {
    s3: (await createS3()).client,
    queues
  })
})

test.beforeEach(async t => {
  for (const [, q] of Object.entries(t.context.queues)) {
    q.queueConsumer.start()
    await pWaitFor(() => q.queueConsumer.isRunning)
  }
})

test.afterEach(async t => {
  for (const [, q] of Object.entries(t.context.queues)) {
    q.queueConsumer.stop()
    await delay(1000)
  }
})

test('can reduce received buffers without creating an aggregate if not enough size', async t => {
  const { s3, queues } = t.context

  const bucketName = await createBucket(s3)
  const { buffers, bufferRecords } = await getBuffers(2)

  const storeClient = createBucketStoreClient(s3, {
    name: bucketName,
    encodeRecord: bufferEncode.storeRecord,
    decodeRecord: bufferDecode.storeRecord,
  })
  const bufferQueueClient = createQueueClient(queues.buffer.sqsClient, {
    queueUrl: queues.buffer.queueUrl,
    encodeMessage: bufferEncode.message,
  })
  const aggregateQueueClient = createQueueClient(queues.aggregate.sqsClient, {
    queueUrl: queues.aggregate.queueUrl,
    encodeMessage: aggregateEncode.message,
  })

  // Store both buffers in store
  await Promise.all(
    buffers.map(b => storeClient.put(b))
  )

  const reduceBufferResp = await reduceBuffer({
    storeClient,
    bufferQueueClient,
    aggregateQueueClient,
    bufferRecords,
    minAggregateSize: 2 ** 34,
    maxAggregateSize: 2 ** 35
  })

  t.falsy(reduceBufferResp.error)
  t.is(reduceBufferResp.ok, 0)

  // Validate message received to queue
  await pWaitFor(() => queues.buffer.queuedMessages.length === 1)

  // New buffer exists
  const bufferRef = await bufferDecode.message(queues.buffer.queuedMessages[0].Body || '')
  const getBufferRes = await storeClient.get(
    `${bufferRef.cid}/${bufferRef.cid}`
  )
  t.truthy(getBufferRes.ok)
  t.falsy(getBufferRes.error)
})

test('can reduce received buffers by creating an aggregate and remaining buffer', async t => {
  const { s3, queues } = t.context

  const bucketName = await createBucket(s3)
  const { buffers, bufferRecords } = await getBuffers(2, {
    length: 10,
    size: 1024
  })

  const storeClient = createBucketStoreClient(s3, {
    name: bucketName,
    encodeRecord: bufferEncode.storeRecord,
    decodeRecord: bufferDecode.storeRecord,
  })
  const bufferQueueClient = createQueueClient(queues.buffer.sqsClient, {
    queueUrl: queues.buffer.queueUrl,
    encodeMessage: bufferEncode.message,
  })
  const aggregateQueueClient = createQueueClient(queues.aggregate.sqsClient, {
    queueUrl: queues.aggregate.queueUrl,
    encodeMessage: aggregateEncode.message,
  })

  // Store both buffers in store
  await Promise.all(
    buffers.map(b => storeClient.put(b))
  )

  const reduceBufferResp = await reduceBuffer({
    storeClient,
    bufferQueueClient,
    aggregateQueueClient,
    bufferRecords,
    minAggregateSize: 2 ** 13,
    maxAggregateSize: 2 ** 15
  })

  t.falsy(reduceBufferResp.error)

  // Validate message received to both queues
  await pWaitFor(() => queues.aggregate.queuedMessages.length === 1)
  await pWaitFor(() => queues.buffer.queuedMessages.length === 1)

  // Validate buffer exists
  const bufferRef = await bufferDecode.message(queues.buffer.queuedMessages[0].Body || '')
  const getBufferRes = await storeClient.get(
    `${bufferRef.cid}/${bufferRef.cid}`
  )
  t.truthy(getBufferRes.ok)
  t.falsy(getBufferRes.error)

  // Validate aggregate reference for buffer exists
  const aggregateRef = await aggregateDecode.message(queues.aggregate.queuedMessages[0].Body || '')
  const getBufferForAggregateRes = await storeClient.get(
    `${aggregateRef.buffer}/${aggregateRef.buffer}`
  )
  t.truthy(getBufferForAggregateRes.ok)
  t.falsy(getBufferForAggregateRes.error)

  // Returns total number of pieces added to aggregate
  t.is(reduceBufferResp.ok, getBufferForAggregateRes.ok?.pieces.length)

  // Validate sum of pieces for aggregate + new buffer are total
  t.is(
    (getBufferRes.ok?.pieces.length || 0) + (getBufferForAggregateRes.ok?.pieces.length || 0),
    buffers.reduce((acc, b) => acc + b.pieces.length, 0)
  )
})

test('can reduce received buffers by creating an aggregate without remaining buffer', async t => {
  const { s3, queues } = t.context

  const bucketName = await createBucket(s3)
  const { buffers, bufferRecords } = await getBuffers(2, {
    length: 100,
    size: 128
  })

  const storeClient = createBucketStoreClient(s3, {
    name: bucketName,
    encodeRecord: bufferEncode.storeRecord,
    decodeRecord: bufferDecode.storeRecord,
  })
  const bufferQueueClient = createQueueClient(queues.buffer.sqsClient, {
    queueUrl: queues.buffer.queueUrl,
    encodeMessage: bufferEncode.message,
  })
  const aggregateQueueClient = createQueueClient(queues.aggregate.sqsClient, {
    queueUrl: queues.aggregate.queueUrl,
    encodeMessage: aggregateEncode.message,
  })

  // Store both buffers in store
  await Promise.all(
    buffers.map(b => storeClient.put(b))
  )

  const reduceBufferResp = await reduceBuffer({
    storeClient,
    bufferQueueClient,
    aggregateQueueClient,
    bufferRecords,
    minAggregateSize: 2 ** 19,
    maxAggregateSize: 2 ** 35
  })
  
  t.falsy(reduceBufferResp.error)

  // Validate message received to both queues
  await pWaitFor(() => queues.aggregate.queuedMessages.length === 1)
  await pWaitFor(() => queues.buffer.queuedMessages.length === 0)

  // Validate aggregate reference for buffer exists
  const aggregateRef = await aggregateDecode.message(queues.aggregate.queuedMessages[0].Body || '')
  const getBufferRes = await storeClient.get(
    `${aggregateRef.buffer}/${aggregateRef.buffer}`
  )
  t.truthy(getBufferRes.ok)
  t.falsy(getBufferRes.error)

  // Returns total number of pieces added to aggregate
  t.is(reduceBufferResp.ok, getBufferRes.ok?.pieces.length)
})

// TODO: merge and remaining

test('fails reducing received buffers if fails to read them from store', async t => {
  const { s3, queues } = t.context

  const bucketName = await createBucket(s3)
  const { buffers, bufferRecords } = await getBuffers(2)

  const storeClient = createBucketStoreClient(s3, {
    name: bucketName,
    encodeRecord: bufferEncode.storeRecord,
    decodeRecord: bufferDecode.storeRecord,
  })
  const bufferQueueClient = createQueueClient(queues.buffer.sqsClient, {
    queueUrl: queues.buffer.queueUrl,
    encodeMessage: bufferEncode.message,
  })
  const aggregateQueueClient = createQueueClient(queues.aggregate.sqsClient, {
    queueUrl: queues.aggregate.queueUrl,
    encodeMessage: aggregateEncode.message,
  })

  // Store ONLY ONE buffer in store
  await storeClient.put(buffers[0])

  const reduceBufferResp = await reduceBuffer({
    storeClient,
    bufferQueueClient,
    aggregateQueueClient,
    bufferRecords,
    minAggregateSize: 128 * 10,
    maxAggregateSize: 2 ** 35
  })

  t.falsy(reduceBufferResp.ok)
  t.truthy(reduceBufferResp.error)
  t.is(reduceBufferResp.error?.name, StoreOperationErrorName)
})

test('fails reducing received buffers if fails to queue buffer', async t => {
  const { s3 } = t.context

  const bucketName = await createBucket(s3)
  const { buffers, bufferRecords } = await getBuffers(2)

  const storeClient = createBucketStoreClient(s3, {
    name: bucketName,
    encodeRecord: bufferEncode.storeRecord,
    decodeRecord: bufferDecode.storeRecord,
  })
  const bufferQueueClient = {
    add: () => {
      return {
        error: new QueueOperationFailed('could not queue buffer')
      }
    }
  }
  const aggregateQueueClient = {
    add: () => {
      return {
        error: new QueueOperationFailed('could not queue aggregate')
      }
    }
  }

  // Store both buffers in store
  await Promise.all(
    buffers.map(b => storeClient.put(b))
  )

  const reduceBufferResp = await reduceBuffer({
    storeClient,
    // @ts-expect-error adapted queue
    bufferQueueClient,
    // @ts-expect-error adapted queue
    aggregateQueueClient,
    bufferRecords,
    minAggregateSize: 128 * 10,
    maxAggregateSize: 2 ** 35
  })

  t.falsy(reduceBufferResp.ok)
  t.truthy(reduceBufferResp.error)
  t.is(reduceBufferResp.error?.name, QueueOperationErrorName)
})

/**
 * @param {number} length
 * @param {object} [piecesOptions]
 * @param {number} [piecesOptions.length]
 * @param {number} [piecesOptions.size]
 */
async function getBuffers (length, piecesOptions = {}) {
  const piecesLength = piecesOptions.length || 100
  const piecesSize = piecesOptions.size || 128

  const pieceBatches = await Promise.all(
    Array.from({ length }).map(() => getPieces(piecesLength, piecesSize)
  ))

  const buffers = await Promise.all(
    pieceBatches.map(async b => buildBuffer(b.pieces)
  ))

  return {
    buffers,
    bufferRecords: await Promise.all(
      buffers.map(bufferEncode.message)
    )
  }
}

/**
 * @param {{ link: import("@web3-storage/data-segment").PieceLink }[]} pieces
 */
function buildBuffer (pieces) {
  return {
    pieces: pieces.map(p => ({
      piece: p.link,
      policy: /** @type {PiecePolicy} */ (0),
      insertedAt: Date.now()
    })),
    storefront: 'did:web:web3.storage',
    group: 'did:web:free.web3.storage',
  }
}

/**
 * @param {number} length 
 * @param {number} size 
 */
async function getPieces (length, size) {
  const pieces = await randomCargo(length, size)

  const pieceRecords = await Promise.all(pieces.map(p => encodePiece(p)))
  return {
    pieces,
    pieceRecords
  }
}

/**
 * @param {{ link: import("@web3-storage/data-segment").PieceLink }} piece
 */
async function encodePiece (piece) {
  const storefront = 'did:web:web3.storage'
  const group = 'did:web:free.web3.storage'
  const pieceRow = {
    piece: piece.link,
    storefront,
    group,
    insertedAt: Date.now()
  }

  return pieceEncode.message(pieceRow)
}
