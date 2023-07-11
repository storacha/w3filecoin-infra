import { testQueue as test } from '../helpers/context.js'

import { createContentQueue } from '../../src/queue/content.js'
import { createPieceQueue } from '../../src/queue/piece.js'
import { createAggregateQueue } from '../../src/queue/aggregate.js'
import { DatabaseValueToUpdateAlreadyTakenErrorName } from '../../src/database/errors.js'

import { createDatabase } from '../helpers/resources.js'
import { getCargo } from '../helpers/cargo.js'

test.beforeEach(async (t) => {
  Object.assign(t.context, {
    dbClient: (await createDatabase()).client,
  })
})

test('can insert to aggregate queue and peek queued content', async t => {
  const { dbClient } = t.context
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const aggregateQueue = createAggregateQueue(dbClient)
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

  // Put aggregate
  const queuePutResp = await aggregateQueue.put(aggregateItem)
  t.truthy(queuePutResp.ok)

  // Peek aggregate
  const queuePeekResp = await aggregateQueue.peek()
  if (!queuePeekResp.ok) {
    throw new Error('no queued items after insert')
  }
  t.is(queuePeekResp.ok.length, 1)

  // Validate Aggregate
  t.truthy(queuePeekResp.ok[0].link.equals(aggregateItem.link))
  t.is(queuePeekResp.ok[0].size, aggregateItem.size)
})

test('when insert to aggregate queue peek from piece queue not return same piece', async t => {
  const { dbClient } = t.context
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const aggregateQueue = createAggregateQueue(dbClient)
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

  // Peek piece before aggregate
  const queuePeekRespBeforePutAggregate = await pieceQueue.peek()
  if (!queuePeekRespBeforePutAggregate.ok) {
    throw new Error('no queued items after insert')
  }

  t.is(queuePeekRespBeforePutAggregate.ok.length, cargoItems.length)

  // Put Aggregate
  const aggregateItem = {
    // TODO: compute commP of commP
    link: cargoItems[0].piece.link,
    size: cargoItems[0].piece.size,
    pieces: cargoItems.map(item => item.piece.link)
  }

  // Put aggregate
  const queuePutResp = await aggregateQueue.put(aggregateItem)
  t.truthy(queuePutResp.ok)

  // Peek piece after aggregate
  const queuePeekRespAfterPutAggregate = await pieceQueue.peek()
  if (!queuePeekRespAfterPutAggregate.ok) {
    throw new Error('no queued items after insert')
  }

  t.is(queuePeekRespAfterPutAggregate.ok.length, 0)
})

test('can insert same batch to the aggregate queue and only peek once', async t => {
  const { dbClient } = t.context
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const aggregateQueue = createAggregateQueue(dbClient)
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

  // Put aggregate
  const queuePutResp0 = await aggregateQueue.put(aggregateItem)
  t.truthy(queuePutResp0.ok)

  // Put same aggregate
  const queuePutResp1 = await aggregateQueue.put(aggregateItem)
  t.truthy(queuePutResp1.ok)

  // Peek aggregate
  const queuePeekResp = await aggregateQueue.peek()
  if (!queuePeekResp.ok) {
    throw new Error('no queued items after insert')
  }
  t.is(queuePeekResp.ok.length, 1)
})

test('fails to put partially same cargo in different aggregate entries', async t => {
  const { dbClient } = t.context
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const aggregateQueue = createAggregateQueue(dbClient)
  const cargoItems = await getCargo(20)
  const aggregatePieces0 = cargoItems.slice(0, 15).map(item => item.piece.link)
  const aggregatePieces1 = cargoItems.slice(4).map(item => item.piece.link)

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

  // Put aggregate with a set of pieces
  const aggregateItem0 = {
    // TODO: compute commP of commP
    link: cargoItems[0].piece.link,
    size: cargoItems[0].piece.size,
    pieces: aggregatePieces0
  }

  const queuePutResp0 = await aggregateQueue.put(aggregateItem0)
  t.truthy(queuePutResp0.ok)

  // Fails to put a second aggregate with partially same pieces
  const aggregateItem1 = {
    // TODO: compute commP of commP
    link: cargoItems[1].piece.link,
    size: cargoItems[1].piece.size,
    pieces: aggregatePieces1
  }

  const queuePutResp1 = await aggregateQueue.put(aggregateItem1)
  t.truthy(queuePutResp1.error)
  t.is(queuePutResp1.error?.name, DatabaseValueToUpdateAlreadyTakenErrorName)

  // Peek aggregate
  const queuePeekResp = await aggregateQueue.peek()
  if (!queuePeekResp.ok) {
    throw new Error('no queued items after insert')
  }
  t.is(queuePeekResp.ok.length, 1)

  // Peek remaining pieces
  const pieceQueuePeekResp = await pieceQueue.peek()
  if (!pieceQueuePeekResp.ok) {
    throw new Error('no queued items after insert')
  }

  t.is(pieceQueuePeekResp.ok.length, 5)
  // Validate pieces
  for(const item of pieceQueuePeekResp.ok) {
    const cargoItem = aggregatePieces1.find(ca => ca.equals(item.piece))
    if (!cargoItem) {
      throw new Error('inserted cargo item should be queued')
    }
    t.truthy(cargoItem)
    t.is(item.priority, 0)
    t.truthy(item.inserted)
  }
})
