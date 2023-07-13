import { testQueue as test } from '../helpers/context.js'

import { createContentQueue } from '../../src/queue/content.js'
import { createPieceQueue } from '../../src/queue/piece.js'
import { DatabaseForeignKeyConstraintErrorName } from '../../src/database/errors.js'

import { createDatabase } from '../helpers/resources.js'
import { getCargo } from '../helpers/cargo.js'

test.beforeEach(async (t) => {
  Object.assign(t.context, {
    dbClient: (await createDatabase()).client,
  })
})

test('can insert to piece queue and peek queued pieces', async t => {
  const { dbClient } = t.context
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const cargoItems = await getCargo(10)

  // Put content
  const contentQueuePutResp = await Promise.all(
    cargoItems.map(item => contentQueue.put(item.content))
  )
  t.falsy(contentQueuePutResp.find(resp => resp.error))

  // Put piece
  const queuePutResp = await Promise.all(cargoItems.map(item => pieceQueue.put({
    link: item.piece.link,
    size: item.piece.size,
    content: item.content.link
  })))
  t.falsy(queuePutResp.find(resp => resp.error))

  // Peek piece
  const queuePeekResp = await pieceQueue.peek()
  if (!queuePeekResp.ok) {
    throw new Error('no queued items after insert')
  }

  t.is(queuePeekResp.ok.length, cargoItems.length)
  for(const item of queuePeekResp.ok) {
    const cargoItem = cargoItems.find(ca => ca.piece.link.equals(item.piece))
    if (!cargoItem) {
      throw new Error('inserted cargo item should be queued')
    }
    t.truthy(cargoItem)
    t.is(item.priority, 0)
    t.deepEqual(item.size, cargoItem.piece.size)
    t.truthy(item.inserted)
  }
})

test('when insert to piece queue peek from content queue not return same content', async t => {
  const { dbClient } = t.context
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const cargoItems = await getCargo(10)

  // Put content
  const contentQueuePutResp = await Promise.all(
    cargoItems.map(item => contentQueue.put(item.content))
  )
  t.falsy(contentQueuePutResp.find(resp => resp.error))

  // Peek content before put piece
  const queuePeekRespBeforePutPiece = await contentQueue.peek()
  if (!queuePeekRespBeforePutPiece.ok) {
    throw new Error('no queued items after insert')
  }

  t.is(queuePeekRespBeforePutPiece.ok.length, cargoItems.length)

  // Put piece
  const queuePutResp = await Promise.all(cargoItems.map(item => pieceQueue.put({
    link: item.piece.link,
    size: item.piece.size,
    content: item.content.link
  })))
  t.falsy(queuePutResp.find(resp => resp.error))

  // Peek content after put piece
  const queuePeekRespAfterPutPiece = await contentQueue.peek()
  if (!queuePeekRespAfterPutPiece.ok) {
    throw new Error('no queued items after insert')
  }

  t.is(queuePeekRespAfterPutPiece.ok.length, 0)
})

test('can insert same batch to the piece queue and only peek once', async t => {
  const { dbClient } = t.context
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const cargoItems = await getCargo(10)

  // Put content
  const contentQueuePutResp = await Promise.all(
    cargoItems.map(item => contentQueue.put(item.content))
  )
  t.falsy(contentQueuePutResp.find(resp => resp.error))

  const pieceItems = cargoItems.map(item => ({
    link: item.piece.link,
    size: item.piece.size,
    content: item.content.link
  }))
  // Put piece
  const queuePutResp0 = await Promise.all(pieceItems.map(item => pieceQueue.put(item)))
  t.falsy(queuePutResp0.find(resp => resp.error))

  // Put same piece
  const queuePutResp1 = await Promise.all(pieceItems.map(item => pieceQueue.put(item)))
  t.falsy(queuePutResp1.find(resp => resp.error))

  // Peek piece
  const queuePeekResp = await pieceQueue.peek()
  if (!queuePeekResp.ok) {
    throw new Error('no queued items after insert')
  }

  t.is(queuePeekResp.ok.length, cargoItems.length)
})

test('can insert partially same batch to the piece queue and only peek once same items', async t => {
  const { dbClient } = t.context
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const cargoItems = await getCargo(20)

  // Put content
  const contentQueuePutResp = await Promise.all(
    cargoItems.map(item => contentQueue.put(item.content))
  )
  t.falsy(contentQueuePutResp.find(resp => resp.error))

  const pieceItems = cargoItems.map(item => ({
    link: item.piece.link,
    size: item.piece.size,
    content: item.content.link
  }))
  const pieceItems0 = pieceItems.slice(0, 15)
  const pieceItems1 = pieceItems.slice(4)

  // Put piece
  const queuePutResp0 = await Promise.all(pieceItems0.map(item => pieceQueue.put(item)))
  t.falsy(queuePutResp0.find(resp => resp.error))

  // Put partially same piece
  const queuePutResp1 = await Promise.all(pieceItems1.map(item => pieceQueue.put(item)))
  t.falsy(queuePutResp1.find(resp => resp.error))

  // Peek piece
  const queuePeekResp = await pieceQueue.peek()
  if (!queuePeekResp.ok) {
    throw new Error('no queued items after insert')
  }

  t.is(queuePeekResp.ok.length, cargoItems.length)
})

test('fails to insert piece if no content exists for given piece', async t => {
  const { dbClient } = t.context
  const pieceQueue = createPieceQueue(dbClient)
  const cargoItems = await getCargo(10)

  // Put piece without content already there
  const queuePutResp = await Promise.all(cargoItems.map(item => pieceQueue.put({
    link: item.piece.link,
    size: item.piece.size,
    content: item.content.link
  })))
  t.falsy(queuePutResp.find(resp => resp.ok))
  t.is(queuePutResp[0].error?.name, DatabaseForeignKeyConstraintErrorName)
})
