import { testWorkflow as test } from '../helpers/context.js'

import { Consumer } from 'sqs-consumer'
import { Aggregate } from '@web3-storage/data-segment'
import pDefer from 'p-defer'
import pWaitFor from 'p-wait-for'

import { createView } from '../../src/database/views.js'
import { createContentQueue } from '../../src/queue/content.js'
import { createPieceQueue } from '../../src/queue/piece.js'
import { createAggregateQueue } from '../../src/queue/aggregate.js'
import { createDealQueue, DEAL, STATUS as DEAL_STATUS } from '../../src/queue/deal.js'
import * as submissionWorkflow from '../../src/workflow/submission.js'

import { createDatabase, createQueue } from '../helpers/resources.js'
import { getCargo } from '../helpers/cargo.js'
import { getAggregateServiceCtx, getAggregateServiceServer } from '../helpers/ucanto.js'

test.beforeEach(async t => {
  // Await for closing sometimes not fast enough
  await new Promise(resolve => setTimeout(() => resolve(true), 500))
  const sqs = await createQueue()

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
    dbClient: (await createDatabase()).client,
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

test('can consume empty aggregate queue', async t => {
  const { dbClient, sqsClient, queueUrl, queueMessages } = t.context

  const aggregateQueue = createAggregateQueue(dbClient)
  const dealQueue = createDealQueue(dbClient)

  const { error, ok } = await submissionWorkflow.consume({
    aggregateQueue,
    sqsClient,
    queueUrl
  })
  t.falsy(error)
  t.is(ok?.count, 0)
  t.is(queueMessages.length, 0)

  // Validate no deal can be peeked
  const queuePeekRespAfterSigned = await dealQueue.peek()
  if (!queuePeekRespAfterSigned.ok) {
    throw new Error('no queued items after insert')
  }
  t.is(queuePeekRespAfterSigned.ok.length, 0)
})

test('can consume aggregate queue with valid aggregates', async t => {
  const { dbClient, sqsClient, queueUrl, queueMessages } = t.context
  const {
    contentQueue,
    pieceQueue,
    aggregateQueue,
  } = await prepare(dbClient)
  const { aggregate } = await addFixtures({ contentQueue, pieceQueue, aggregateQueue })

  const { error, ok } = await submissionWorkflow.consume({
    aggregateQueue,
    sqsClient,
    queueUrl
  })
  t.falsy(error)
  t.is(ok?.count, 1)
  await pWaitFor(
    () => ok?.count === queueMessages.length,
    { interval: 200 }
  )

  // Validate messages
  const aggregateItemsToProcess = queueMessages.map(qm => submissionWorkflow.decode(qm.Body || ''))
  for (const item of aggregateItemsToProcess) {
    t.truthy(aggregate.link.equals(item.link))
  }
})

test('can consume aggregate queue and write to builder queue', async t => {
  const { dbClient, sqsClient, queueUrl, queueMessages } = t.context
  const {
    contentQueue,
    pieceQueue,
    aggregateQueue,
    dealQueue,
    databaseView,
    aggregationService,
    storefront
  } = await prepare(dbClient)
  const { aggregate } = await addFixtures({ contentQueue, pieceQueue, aggregateQueue })

  // Consume
  const { error: consumerError, ok: consumeOk } = await submissionWorkflow.consume({
    aggregateQueue,
    sqsClient,
    queueUrl
  })
  t.falsy(consumerError)
  await pWaitFor(
    () => consumeOk?.count === queueMessages.length,
    { interval: 200 }
  )

  // Create Ucanto service
  const aggregateOfferCall = pDefer()
  const serviceServer = await getAggregateServiceServer(aggregationService.raw, {
    onCall: (invCap) => {
      aggregateOfferCall.resolve(invCap)
    }
  })

  // Build offer
  const { error } = await submissionWorkflow.buildOffer({
    item: queueMessages[0].Body || '',
    dealQueue,
    databaseView,
    did: storefront.DID,
    privateKey: storefront.PRIVATE_KEY,
    aggregationServiceConnection: serviceServer.connection
  })
  t.falsy(error)

  // Validate ucanto server call
  t.is(serviceServer.service.aggregate.offer.callCount, 1)
  const invCap = await aggregateOfferCall.promise
  t.is(invCap.can, 'aggregate/offer')
  // TODO: bigint
  t.is(invCap.nb.piece.size, Number(aggregate.size))

  // Peek deal should still be empty until signed
  const queuePeekRespBeforeSigned = await dealQueue.peek()
  if (!queuePeekRespBeforeSigned.ok) {
    throw new Error('no queued items response')
  }
  t.is(queuePeekRespBeforeSigned.ok.length, 0)

  // Set deal as signed
  await dbClient
   .updateTable(DEAL)
   .set({
     status: DEAL_STATUS.SIGNED
   })
   .where('aggregate', '=', aggregate.link.toString())
   .execute()

  // Peek deal should after signed
  const queuePeekRespAfterSigned = await dealQueue.peek()
  if (!queuePeekRespAfterSigned.ok) {
    throw new Error('no queued items after insert')
  }
  t.is(queuePeekRespAfterSigned.ok.length, 1)
})

test('can build items gracefully when concurrently handling messages', async t => {
  const { dbClient, sqsClient, queueUrl, queueMessages } = t.context
  const {
    contentQueue,
    pieceQueue,
    aggregateQueue,
    dealQueue,
    databaseView,
    aggregationService,
    storefront
  } = await prepare(dbClient)
  await addFixtures({ contentQueue, pieceQueue, aggregateQueue })

  // Consume
  const { error: consumerError, ok: consumeOk } = await submissionWorkflow.consume({
    aggregateQueue,
    sqsClient,
    queueUrl
  })
  t.falsy(consumerError)
  await pWaitFor(
    () => consumeOk?.count === queueMessages.length,
    { interval: 200 }
  )

  // Create Ucanto service
  const aggregateOfferCall = pDefer()
  const serviceServer = await getAggregateServiceServer(aggregationService.raw, {
    onCall: (invCap) => {
      aggregateOfferCall.resolve(invCap)
    }
  })

  // Build offer concurrently
  const res = await Promise.all([
    submissionWorkflow.buildOffer({
      item: queueMessages[0].Body || '',
      dealQueue,
      databaseView,
      did: storefront.DID,
      privateKey: storefront.PRIVATE_KEY,
      aggregationServiceConnection: serviceServer.connection
    }),
    submissionWorkflow.buildOffer({
      item: queueMessages[0].Body || '',
      dealQueue,
      databaseView,
      did: storefront.DID,
      privateKey: storefront.PRIVATE_KEY,
      aggregationServiceConnection: serviceServer.connection
    })
  ])
  t.falsy(res.find(r => r.error))
})

/**
 * @param {import('kysely').Kysely<import('../../src/schema.js').Database>} dbClient
 */
async function prepare (dbClient) {
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const aggregateQueue = createAggregateQueue(dbClient)
  const dealQueue = createDealQueue(dbClient)
  const databaseView = createView(dbClient)

  const { storefront, aggregationService } = await getAggregateServiceCtx()

  return {
    contentQueue,
    pieceQueue,
    aggregateQueue,
    dealQueue,
    databaseView,
    storefront,
    aggregationService
  }
}

/**
 * @param {object} props
 * @param {import('../../src/types').ContentQueue} props.contentQueue
 * @param {import('../../src/types').PieceQueue} props.pieceQueue
 * @param {import('../../src/types').AggregateQueue} props.aggregateQueue
 */
async function addFixtures ({ contentQueue, pieceQueue, aggregateQueue }) {
  // Prepare pieces by adding to content queue, then piece queue and then aggregate queue
  const cargoItems = await getCargo(10)
  const contentQueuePutResp = await Promise.all(
    cargoItems.map(item => contentQueue.put(item.content))
  )
  if (contentQueuePutResp.some(resp => resp.error)) {
    throw new Error('could not write to content queue')
  }

  const pieceQueuePutResp = await Promise.all(cargoItems.map(item => pieceQueue.put({
    link: item.piece.link,
    // Write bumped size for testing purposes
    size: item.piece.size,
    content: item.content.link
  })))
  if (pieceQueuePutResp.some(resp => resp.error)) {
    throw new Error('could not write to piece queue')
  }

  // Create aggregate
  const builder = Aggregate.createBuilder()
  const addedPieces = []

  for (const item of cargoItems) {
    try {
      builder.write({
        root: item.piece.link.multihash.digest,
        size: item.piece.size
      })
      addedPieces.push(item.piece.link)
    } catch {}
  }

  const aggregateBuild = builder.build()
  const aggregate = {
    link: aggregateBuild.link(),
    size: aggregateBuild.size,
    pieces: addedPieces
  }
  const aggregateQueuePutResp = await aggregateQueue.put(aggregate)
  if (aggregateQueuePutResp.error) {
    throw new Error('could not write to aggregate queue')
  }

  return {
    cargoItems,
    aggregate
  }
}
