import { tesWorkflow as test } from '../helpers/context.js'
import { createQueue } from '../helpers/resources.js'
import { randomCargo } from '../helpers/cargo.js'
import { getAggregatorServiceServer, getAggregatorServiceCtx } from '../helpers/ucanto.js'

import { Consumer } from 'sqs-consumer'
import pWaitFor from 'p-wait-for'
import delay from 'delay'
import pDefer from 'p-defer'
import { QueueOperationFailed } from '@web3-storage/filecoin-api-legacy/errors'

import { getServiceSigner } from '../../src/service.js'
import { encode as pieceEncode } from '../../src/data/piece.js'
import { createQueueClient } from '../../src/queue/client.js'

import { addPieces } from '../../src/workflow/piece-add.js'

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
  await delay(1000)
  t.context.queueConsumer.start()
  await pWaitFor(() => t.context.queueConsumer.isRunning)
})

test.afterEach(async t => {
  t.context.queueConsumer.stop()
  await delay(3000)
})

test.skip('can add received pieces', async t => {
  const { sqsClient, queueUrl, queuedMessages } = t.context

  const { pieces, pieceRecords } = await getPieces(2, 128)
  const queueClient = createQueueClient(sqsClient, {
    queueUrl,
    encodeMessage: pieceEncode.message,
  })

  const aggregatorAddCall = pDefer()
  const { invocationConfig, aggregatorService } = await getService({
    onCall: aggregatorAddCall
  })

  const aggregatorAddResp = await addPieces({
    queueClient,
    invocationConfig,
    aggregatorServiceConnection: aggregatorService.connection,
    records: pieceRecords.map((pr, index) => ({
      body: pr,
      id: `${index}`
    }))
  })

  t.truthy(aggregatorAddResp.ok)
  t.falsy(aggregatorAddResp.error)

  // Validate ucanto server call
  const invCap = await aggregatorAddCall.promise
  t.is(aggregatorService.service.aggregate.add.callCount, pieces.length)
  t.is(invCap.can, 'aggregate/add')

  // Validate messages received to queue
  await pWaitFor(() => queuedMessages.length === pieces.length)
})

test.skip('handles partial fails when received same pieces and fails to add them', async t => {
  const { sqsClient, queueUrl, queuedMessages } = t.context

  const { pieces, pieceRecords } = await getPieces(4, 128)
  // Creating two slices of pieces with common intersection - 2 pieces
  const pieceRecordsA = pieceRecords.slice(0, (pieceRecords.length / 2) + 1)
  const pieceRecordsB = pieceRecords.slice((pieceRecords.length / 2) - 1)

  // Create context
  const queueClient = createQueueClient(sqsClient, {
    queueUrl,
    encodeMessage: pieceEncode.message,
  })

  const seenPieces = new Set()
  const aggregatorAddCall = pDefer()
  const { invocationConfig, aggregatorService } = await getService({
    onCall: aggregatorAddCall,
    // Fails if it already has piece
    shouldFail: (invCap) => {
      const pieceString = invCap.nb.piece.toString()
      if (seenPieces.has(pieceString)) {
        return true
      }
      seenPieces.add(pieceString)
      return false
    }
  })

  const aggregatorQueueRespA = await addPieces({
    queueClient,
    invocationConfig,
    aggregatorServiceConnection: aggregatorService.connection,
    records: pieceRecordsA.map((pr, index) => ({
      body: pr,
      id: `${index}`
    }))
  })

  t.truthy(aggregatorQueueRespA.ok)
  t.falsy(aggregatorQueueRespA.error)

  // Validate ucanto server call
  const invCap = await aggregatorAddCall.promise
  t.is(aggregatorService.service.aggregate.add.callCount, pieceRecordsA.length)
  t.is(invCap.can, 'aggregate/add')

  // Validate messages received to queue
  await pWaitFor(() => queuedMessages.length === pieceRecordsA.length)

  const aggregatorQueueRespB = await addPieces({
    queueClient,
    invocationConfig,
    aggregatorServiceConnection: aggregatorService.connection,
    records: pieceRecordsB.map((pr, index) => ({
      body: pr,
      id: `${index}`
    }))
  })

  t.falsy(aggregatorQueueRespB.ok)
  t.truthy(aggregatorQueueRespB.error)
  t.is(aggregatorQueueRespB.error?.length, (pieceRecordsB.length + pieceRecordsA.length) - pieces.length)
  t.deepEqual(
    aggregatorQueueRespB.error?.map(e => e?.id),
    Array.from({ length: (pieceRecordsB.length + pieceRecordsA.length) - pieces.length }, (_, i) => `${i}`)
  )

  // Validate messages received to queue
  await pWaitFor(() => queuedMessages.length === pieces.length)
})

test.skip('handles failures when received same pieces and fails to queue them for buffering', async t => {
  const { pieces, pieceRecords } = await getPieces(4, 128)

  // Create context
  const queueClient = {
    add: () => {
      return {
        error: new QueueOperationFailed('could not queue buffer')
      }
    }
  }

  const aggregatorAddCall = pDefer()
  const { invocationConfig, aggregatorService } = await getService({
    onCall: aggregatorAddCall
  })

  const aggregatorQueueResp = await addPieces({
    // @ts-expect-error adapted queue
    queueClient,
    invocationConfig,
    aggregatorServiceConnection: aggregatorService.connection,
    records: pieceRecords.map((pr, index) => ({
      body: pr,
      id: `${index}`
    }))
  })

  t.falsy(aggregatorQueueResp.ok)
  t.truthy(aggregatorQueueResp.error)
  t.is(aggregatorQueueResp.error?.length, pieces.length)
  t.deepEqual(
    aggregatorQueueResp.error?.map(e => e?.id),
    Array.from({ length: pieces.length }, (_, i) => `${i}`)
  )
})

/**
 * @param {object} options
 * @param {import('p-defer').DeferredPromise<any>} options.onCall
 * @param {(inCap: any) => boolean} [options.shouldFail]
 */
async function getService (options) {
  const { aggregator } = await getAggregatorServiceCtx()
  const aggregatorService = await getAggregatorServiceServer(aggregator.raw, {
    onCall: (invCap) => {
      options.onCall.resolve(invCap)
    },
    shouldFail: options.shouldFail
  })
  const issuer = getServiceSigner(aggregator)
  const audience = aggregatorService.connection.id
  /** @type {import('@web3-storage/filecoin-client/types').InvocationConfig} */
  const invocationConfig = {
    issuer,
    audience,
    with: issuer.did(),
  }

  return {
    invocationConfig,
    aggregatorService
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
