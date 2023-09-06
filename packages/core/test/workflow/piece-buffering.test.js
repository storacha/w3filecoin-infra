import { tesWorkflow as test } from '../helpers/context.js'
import { createS3, createBucket, createQueue } from '../helpers/resources.js'
import { randomCargo } from '../helpers/cargo.js'

import { Consumer } from 'sqs-consumer'
import pWaitFor from 'p-wait-for'
import delay from 'delay'
import { StoreOperationFailed, StoreOperationErrorName, QueueOperationFailed, QueueOperationErrorName } from '@web3-storage/filecoin-api/errors'

import { encode as pieceEncode } from '../../src/data/piece.js'
import { encode as bufferEncode, decode as bufferDecode } from '../../src/data/buffer.js'
import { createBucketStoreClient } from '../../src/store/bucket-client.js'
import { createQueueClient } from '../../src/queue/client.js'

import { bufferPieces } from '../../src/workflow/piece-buffering.js'

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
    s3: (await createS3()).client,
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

test('can buffer received pieces', async t => {
  const { s3, sqsClient, queueUrl, queuedMessages } = t.context
  const bucketName = await createBucket(s3)
  const { pieces, pieceRecords } = await getPieces(100, 128)

  const storeClient = createBucketStoreClient(s3, {
    name: bucketName,
    encodeRecord: bufferEncode.storeRecord,
    decodeRecord: bufferDecode.storeRecord,
  })
  const queueClient = createQueueClient(sqsClient, {
    queueUrl,
    encodeMessage: bufferEncode.message,
  })

  const bufferPiecesResp = await bufferPieces({
    storeClient,
    queueClient,
    records: pieceRecords.map((pr, index) => ({
      body: pr,
      id: `${index}`
    })),
    // we cannot get elasticmq to be FIFO with SQS create command
    disableMessageGroupId: true
  })
  t.truthy(bufferPiecesResp.ok)
  t.falsy(bufferPiecesResp.error)
  t.is(bufferPiecesResp.ok?.countSuccess, pieces.length)

  // Validate message received to queue
  await pWaitFor(() => queuedMessages.length === 1)

  const bufferRef = await bufferDecode.message(queuedMessages[0].Body || '')
  const getBufferRes = await storeClient.get(
    `${bufferRef.cid}/${bufferRef.cid}`
  )
  t.truthy(getBufferRes.ok)
  t.falsy(getBufferRes.error)
  t.is(getBufferRes.ok?.pieces.length, pieces.length)

  for (const bufferedPiece of getBufferRes.ok?.pieces || []) {
    t.truthy(pieces.find(piece => piece.link.equals(bufferedPiece.piece)))
    t.is(bufferedPiece.policy, 0)
  }
})

test('can buffer received pieces with different groups', async t => {
  const { s3, sqsClient, queueUrl, queuedMessages } = t.context
  const storefronts = [
    'did:web:web.storage',
    'did:web:nft.storage'
  ]
  const bucketName = await createBucket(s3)
  const {
    pieces: piecesStorefrontA,
    pieceRecords: pieceRecordsStorefrontA, 
  } = await getPieces(50, 128, {
    storefront: storefronts[0]
  })
  const {
    pieces: piecesStorefrontB,
    pieceRecords: pieceRecordsStorefrontB
  } = await getPieces(50, 128, {
    storefront: storefronts[1]
  })

  const storeClient = createBucketStoreClient(s3, {
    name: bucketName,
    encodeRecord: bufferEncode.storeRecord,
    decodeRecord: bufferDecode.storeRecord,
  })
  const queueClient = createQueueClient(sqsClient, {
    queueUrl,
    encodeMessage: bufferEncode.message,
  })

  const bufferPiecesResp = await bufferPieces({
    storeClient,
    queueClient,
    records: [...pieceRecordsStorefrontA, ...pieceRecordsStorefrontB].map((pr, index) => ({
      body: pr,
      id: `${index}`
    })),
    // we cannot get elasticmq to be FIFO with SQS create command
    disableMessageGroupId: true
  })
  t.truthy(bufferPiecesResp.ok)
  t.falsy(bufferPiecesResp.error)
  t.is(bufferPiecesResp.ok?.countSuccess, [...piecesStorefrontA, ...piecesStorefrontB].length)

  // Validate message received to queue
  await pWaitFor(() => queuedMessages.length === 2)

  // Storefront message 0
  const bufferRef0 = await bufferDecode.message(queuedMessages[0].Body || '')
  const getBufferRes0 = await storeClient.get(
    `${bufferRef0.cid}/${bufferRef0.cid}`
  )
  t.truthy(getBufferRes0.ok)
  t.falsy(getBufferRes0.error)

  const piecesStorefrontMessage0 = getBufferRes0.ok?.storefront === storefronts[0] ? piecesStorefrontA : piecesStorefrontB
  t.is(getBufferRes0.ok?.pieces.length, piecesStorefrontMessage0.length)

  for (const bufferedPiece of getBufferRes0.ok?.pieces || []) {
    t.truthy(
      piecesStorefrontMessage0.find(piece => piece.link.equals(bufferedPiece.piece))
    )
    t.is(bufferedPiece.policy, 0)
  }

  // Storefront message 1
  const bufferRef1 = await bufferDecode.message(queuedMessages[1].Body || '')
  const getBufferRes1 = await storeClient.get(
    `${bufferRef1.cid}/${bufferRef1.cid}`
  )
  t.truthy(getBufferRes1.ok)
  t.falsy(getBufferRes1.error)

  const piecesStorefrontMessage1 = getBufferRes1.ok?.storefront === storefronts[0] ? piecesStorefrontA : piecesStorefrontB
  t.is(getBufferRes1.ok?.pieces.length, piecesStorefrontB.length)

  for (const bufferedPiece of getBufferRes1.ok?.pieces || []) {
    t.truthy(piecesStorefrontMessage1.find(piece => piece.link.equals(bufferedPiece.piece)))
    t.is(bufferedPiece.policy, 0)
  }
})

test('fails buffering received pieces if fails to store', async t => {
  const { sqsClient, queueUrl } = t.context
  const { pieceRecords } = await getPieces(100, 128)

  const storeClient = {
    put: () => {
      return {
        error: new StoreOperationFailed('could not store buffer')
      }
    }
  }
  const queueClient = createQueueClient(sqsClient, {
    queueUrl,
    encodeMessage: bufferEncode.message,
  })

  const bufferPiecesResp = await bufferPieces({
    // @ts-expect-error 
    storeClient,
    queueClient,
    records: pieceRecords.map((pr, index) => ({
      body: pr,
      id: `${index}`
    })),
    disableMessageGroupId: true
  })
  t.falsy(bufferPiecesResp.ok)
  t.truthy(bufferPiecesResp.error)
  t.is(bufferPiecesResp.error?.name, StoreOperationErrorName)
})

test('fails buffering received pieces if fails to queue', async t => {
  const { s3 } = t.context
  const bucketName = await createBucket(s3)
  const { pieceRecords } = await getPieces(100, 128)

  const storeClient = createBucketStoreClient(s3, {
    name: bucketName,
    encodeRecord: bufferEncode.storeRecord,
    decodeRecord: bufferDecode.storeRecord,
  })
  const queueClient = {
    add: () => {
      return {
        error: new QueueOperationFailed('could not queue buffer')
      }
    }
  }

  const bufferPiecesResp = await bufferPieces({
    storeClient,
    // @ts-expect-error adapted queue
    queueClient,
    records: pieceRecords.map((pr, index) => ({
      body: pr,
      id: `${index}`
    })),
    disableMessageGroupId: true
  })
  t.falsy(bufferPiecesResp.ok)
  t.truthy(bufferPiecesResp.error)
  t.is(bufferPiecesResp.error?.name, QueueOperationErrorName)
})

/**
 * @param {number} length
 * @param {number} size
 * @param {object} [opts]
 * @param {string} [opts.storefront]
 * @param {string} [opts.group]
 */
async function getPieces (length, size, opts = {}) {
  const pieces = await randomCargo(length, size)

  const pieceRecords = await Promise.all(pieces.map(p => encodePiece(p, opts)))
  return {
    pieces,
    pieceRecords
  }
}

/**
 * @param {{ link: import("@web3-storage/data-segment").PieceLink }} piece
 * @param {object} [opts]
 * @param {string} [opts.storefront]
 * @param {string} [opts.group]
 */
async function encodePiece (piece, opts = {}) {
  const storefront = opts.storefront || 'did:web:web3.storage'
  const group = opts.group || 'did:web:free.web3.storage'
  const pieceRow = {
    piece: piece.link,
    storefront,
    group,
    insertedAt: Date.now()
  }

  return pieceEncode.message(pieceRow)
}
