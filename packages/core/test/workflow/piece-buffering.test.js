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

test.before(async (t) => {
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
  const { pieces, pieceRecords } = await getPieces()

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
    pieceRecords,
  })
  t.truthy(bufferPiecesResp.ok)
  t.falsy(bufferPiecesResp.error)
  t.is(bufferPiecesResp.ok, pieces.length)

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

test('fails buffering received pieces if fails to store', async t => {
  const { sqsClient, queueUrl } = t.context
  const { pieceRecords } = await getPieces()

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
    pieceRecords,
  })
  t.falsy(bufferPiecesResp.ok)
  t.truthy(bufferPiecesResp.error)
  t.is(bufferPiecesResp.error?.name, StoreOperationErrorName)
})

test('fails buffering received pieces if fails to queue', async t => {
  const { s3 } = t.context
  const bucketName = await createBucket(s3)
  const { pieceRecords } = await getPieces()

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
    pieceRecords,
  })
  t.falsy(bufferPiecesResp.ok)
  t.truthy(bufferPiecesResp.error)
  t.is(bufferPiecesResp.error?.name, QueueOperationErrorName)
})

async function getPieces () {
  const pieces = await randomCargo(100, 128)

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
