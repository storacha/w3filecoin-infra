import { testQueue as test } from '../helpers/context.js'

import { createContentQueue } from '../../src/queue/content.js'
import { createPieceQueue } from '../../src/queue/piece.js'
import { createAggregateQueue } from '../../src/queue/aggregate.js'
import { createDealQueue, DEAL, STATUS as DEAL_STATUS } from '../../src/queue/deal.js'

import { createDatabase } from '../helpers/resources.js'
import { getCargo } from '../helpers/cargo.js'

test.beforeEach(async (t) => {
  Object.assign(t.context, {
    dbClient: (await createDatabase()).client,
  })
})

test('can insert to deal queue and peek queued deals', async t => {
  const { dbClient } = t.context
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const aggregateQueue = createAggregateQueue(dbClient)
  const dealQueue = createDealQueue(dbClient)
  const cargoItems = await getCargo(10)

  // Put content
  const contentQueuePutResp = await Promise.all(
    cargoItems.map(item => contentQueue.put(item.content))
  )
  t.falsy(contentQueuePutResp.find(resp => resp.error))

  // Put piece
  const pieceQueuePutResp = await Promise.all(cargoItems.map(item => pieceQueue.put({
    link: item.piece.link,
    size: item.piece.size,
    content: item.content.link
  })))
  t.falsy(pieceQueuePutResp.find(resp => resp.error))

  // Put Aggregate
  const aggregateItem = {
    // TODO: compute commP of commP
    link: cargoItems[0].piece.link,
    size: cargoItems[0].piece.size,
    pieces: cargoItems.map(item => item.piece.link)
  }

  const aggregateQueuePutResp = await aggregateQueue.put(aggregateItem)
  t.truthy(aggregateQueuePutResp.ok)

  // Put deal
  const dealItem = {
    aggregate: aggregateItem.link
  }
  const queuePutResp = await dealQueue.put(dealItem)
  t.truthy(queuePutResp.ok)

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
    .where('aggregate', '=', aggregateItem.link.toString())
    .execute()

  // Peek deal should after signed
  const queuePeekRespAfterSigned = await dealQueue.peek()
  if (!queuePeekRespAfterSigned.ok) {
    throw new Error('no queued items after insert')
  }
  t.is(queuePeekRespAfterSigned.ok.length, 1)
})

test('when insert to deal queue peek from aggregate queue not return same aggregate', async t => {
  const { dbClient } = t.context
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const aggregateQueue = createAggregateQueue(dbClient)
  const dealQueue = createDealQueue(dbClient)
  const cargoItems = await getCargo(10)

  // Put content
  const contentQueuePutResp = await Promise.all(
    cargoItems.map(item => contentQueue.put(item.content))
  )
  t.falsy(contentQueuePutResp.find(resp => resp.error))

  // Put piece
  const pieceQueuePutResp = await Promise.all(cargoItems.map(item => pieceQueue.put({
    link: item.piece.link,
    size: item.piece.size,
    content: item.content.link
  })))
  t.falsy(pieceQueuePutResp.find(resp => resp.error))

  // Put Aggregate
  const aggregateItem = {
    // TODO: compute commP of commP
    link: cargoItems[0].piece.link,
    size: cargoItems[0].piece.size,
    pieces: cargoItems.map(item => item.piece.link)
  }

  const aggregateQueuePutResp = await aggregateQueue.put(aggregateItem)
  t.truthy(aggregateQueuePutResp.ok)

  // Peek aggregate before deal
  const queuePeekRespBeforePutDeal = await aggregateQueue.peek()
  if (!queuePeekRespBeforePutDeal.ok) {
    throw new Error('no queued items after insert')
  }
  t.is(queuePeekRespBeforePutDeal.ok.length, 1)

  // Put deal
  const dealItem = {
    aggregate: aggregateItem.link
  }
  const queuePutResp = await dealQueue.put(dealItem)
  t.truthy(queuePutResp.ok)

  // Peek aggregate after deal
  const queuePeekRespAfterPutDeal = await aggregateQueue.peek()
  if (!queuePeekRespAfterPutDeal.ok) {
    throw new Error('no queued items after insert')
  }
  t.is(queuePeekRespAfterPutDeal.ok.length, 0)
})

test('can insert same batch to the deal queue and only peek once', async t => {
  const { dbClient } = t.context
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const aggregateQueue = createAggregateQueue(dbClient)
  const dealQueue = createDealQueue(dbClient)
  const cargoItems = await getCargo(10)

  // Put content
  const contentQueuePutResp = await Promise.all(
    cargoItems.map(item => contentQueue.put(item.content))
  )
  t.falsy(contentQueuePutResp.find(resp => resp.error))

  // Put piece
  const pieceQueuePutResp = await Promise.all(cargoItems.map(item => pieceQueue.put({
    link: item.piece.link,
    size: item.piece.size,
    content: item.content.link
  })))
  t.falsy(pieceQueuePutResp.find(resp => resp.error))

  // Put Aggregate
  const aggregateItem = {
    // TODO: compute commP of commP
    link: cargoItems[0].piece.link,
    size: cargoItems[0].piece.size,
    pieces: cargoItems.map(item => item.piece.link)
  }

  const aggregateQueuePutResp = await aggregateQueue.put(aggregateItem)
  t.truthy(aggregateQueuePutResp.ok)

  // Put deal
  const dealItem = {
    aggregate: aggregateItem.link
  }
  const queuePutResp0 = await dealQueue.put(dealItem)
  t.truthy(queuePutResp0.ok)

  // Put same deal
  const queuePutResp1 = await dealQueue.put(dealItem)
  t.truthy(queuePutResp1.ok)

   // Peek deal should still be empty until signed
   const queuePeekResp = await dealQueue.peek()
   if (!queuePeekResp.ok) {
     throw new Error('no queued items after insert')
   }
   t.is(queuePeekResp.ok.length, 0)
})
