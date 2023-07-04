import { test } from './helpers/context.js'

import { useAggregateTable } from '../src/table/aggregate.js'
import { useContentTable } from '../src/table/content.js'
import { usePieceTable } from '../src/table/piece.js'
import { useDealTable } from '../src/table/deal.js'
import { useCargoView } from '../src/views/cargo.js'
import { useDealView } from '../src/views/deal.js'
import { useContentQueueView } from '../src/views/content-queue.js'
import { useAggregateQueueView } from '../src/views/aggregate-queue.js'
import { DatabaseUniqueValueConstraintErrorName } from '../src/table/errors.js'

import { createDatabase } from './helpers/resources.js'
import { getCargo } from './helpers/cargo.js'

test.beforeEach(async (t) => {
  Object.assign(t.context, {
    dbClient: (await createDatabase()).client,
  })
})

test('can insert to content table and get content queued', async t => {
  const { dbClient } = t.context
  const contentTable = useContentTable(dbClient)
  const contentQueueView = useContentQueueView(dbClient)
  const cargoItems = await getCargo(10)

  const insertOps = await Promise.all(
    cargoItems.map(cargo => contentTable.insert(cargo.content))
  )
  for (const op of insertOps) {
    t.truthy(op.ok)
  }

  const queuedItems = await contentQueueView.select()
  if (!queuedItems.ok) {
    throw new Error('no queued items after insert')
  }
  t.falsy(queuedItems.error)
  t.is(queuedItems.ok.length, cargoItems.length)

  for(const item of queuedItems.ok) {
    const cargoItem = cargoItems.find(ca => ca.content.link.equals(item.link))
    if (!cargoItem) {
      throw new Error('inserted cargo item should be queued')
    }
    t.truthy(cargoItem)
    t.is(item.size, cargoItem.content.size)
    t.is(item.bucketName, cargoItem.content.bucketName)
    t.is(item.bucketEndpoint, cargoItem.content.bucketEndpoint)
    t.truthy(item.inserted)
  }
})

test('can set content to the queue and set their piece', async t => {
  const { dbClient } = t.context
  const contentTable = useContentTable(dbClient)
  const pieceTable = usePieceTable(dbClient)
  const contentQueueView = useContentQueueView(dbClient)
  const cargoView = useCargoView(dbClient)
  const cargoItems = await getCargo(10)

  // Queue content to get piece
  await Promise.all(
    cargoItems.map(cargo => contentTable.insert(cargo.content))
  )
  // Async piece computation can happen by reading from the queue
  const queuedItemsBeforePieceCompute = await contentQueueView.select()
  t.is(queuedItemsBeforePieceCompute.ok?.length, cargoItems.length)

  // No cargo is ready for being aggregated
  const queuedCargoToAggregationBeforePieceCompute = await cargoView.select()
  t.is(queuedCargoToAggregationBeforePieceCompute.ok?.length, 0)

  // Fill in pieces for content entries
  const insertOps = await Promise.all(
    cargoItems.map(cargo => pieceTable.insert(cargo.piece, cargo.content.link))
  )
  for (const op of insertOps) {
    t.truthy(op.ok)
  }

  // Content queue is handled
  const queuedItemsAfterPieceCompute = await contentQueueView.select()
  t.is(queuedItemsAfterPieceCompute.ok?.length, 0)

  // Cargo queue has entries to be aggregated
  const queuedCargoToAggregationAfterPieceCompute = await cargoView.select()
  t.is(queuedCargoToAggregationAfterPieceCompute.ok?.length, cargoItems.length)
})

test('can create aggregate with queued cargo and add it to pending deals', async t => {
  const { dbClient } = t.context
  const aggregateTable = useAggregateTable(dbClient)
  const contentTable = useContentTable(dbClient)
  const pieceTable = usePieceTable(dbClient)
  const dealTable = useDealTable(dbClient)
  const cargoView = useCargoView(dbClient)
  const dealView = useDealView(dbClient)
  const aggregateQueueView = useAggregateQueueView(dbClient)
  const cargoItems = await getCargo(10)

  // Queue content to get piece
  await Promise.all(
    cargoItems.map(cargo => contentTable.insert(cargo.content))
  )
  // Fill in pieces for content entries
  await Promise.all(
    cargoItems.map(cargo => pieceTable.insert(cargo.piece, cargo.content.link))
  )

  // Cargo queue has entries to be aggregated
  const queuedCargoToAggregationBeforeAggregate = await cargoView.select()
  if (!queuedCargoToAggregationBeforeAggregate.ok) {
    throw new Error('no queued cargo to aggregate')
  }
  t.is(queuedCargoToAggregationBeforeAggregate.ok.length, cargoItems.length)

  // Create aggregate with queued items
  const aggregatePiece = {
    link: cargoItems[0].piece.link,
    size: cargoItems[0].piece.size
  }
  const pieces = queuedCargoToAggregationBeforeAggregate.ok.map(cargo => cargo.piece)
  await aggregateTable.insert(aggregatePiece, pieces)

  // Cargo queue entries not available to aggregate anymore
  const queuedCargoToAggregationAfterAggregate = await cargoView.select()
  t.is(queuedCargoToAggregationAfterAggregate.ok?.length, 0)

  // A queued aggregate was created
  const queuedAggregateBeforeDeal = await aggregateQueueView.select()
  if (!queuedAggregateBeforeDeal.ok) {
    throw new Error('no queued aggregate created')
  }
  t.is(queuedAggregateBeforeDeal.ok.length, 1)

  // Sent a deal from the queue
  await dealTable.insert({
    aggregate: queuedAggregateBeforeDeal.ok[0].link
  })

  // Queued aggregates sent
  const queuedAggregateAfterDeal = await aggregateQueueView.select()
  t.is(queuedAggregateAfterDeal.ok?.length, 0)

  // A deal as pending should have been created
  const pendingDeals = await dealView.selectPending()
  if (!pendingDeals.ok) {
    throw new Error('no pending deals were created')
  }
  t.is(pendingDeals.ok.length, 1)
})

test('concurrent piece insertion fail to be added', async t => {
  const { dbClient } = t.context
  const contentTable = useContentTable(dbClient)
  const pieceTable = usePieceTable(dbClient)
  const cargoItems = await getCargo(10)

  // Queue content to get piece
  await Promise.all(
    cargoItems.map(cargo => contentTable.insert(cargo.content))
  )
  // Fill in pieces for content entries 2x, failign concurrent second ops
  const res = await Promise.all([
    ...cargoItems.map(cargo => pieceTable.insert(cargo.piece, cargo.content.link)),
    ...cargoItems.map(cargo => pieceTable.insert(cargo.piece, cargo.content.link))
  ])
  const okResponses = res.filter(r => r.ok)
  const errorResponses = res.filter(r => r.error)

  t.is(okResponses.length, cargoItems.length)
  t.is(errorResponses.length, cargoItems.length)
  t.is(errorResponses[0].error?.name, DatabaseUniqueValueConstraintErrorName)
})

test('concurrent same aggregate insertion fail to be added a second time', async t => {
  const { dbClient } = t.context
  const aggregateTable = useAggregateTable(dbClient)
  const contentTable = useContentTable(dbClient)
  const pieceTable = usePieceTable(dbClient)
  const cargoView = useCargoView(dbClient)
  const cargoItems = await getCargo(10)

  // Queue content to get piece
  await Promise.all(
    cargoItems.map(cargo => contentTable.insert(cargo.content))
  )
  // Fill in pieces for content entries
  await Promise.all(
    cargoItems.map(cargo => pieceTable.insert(cargo.piece, cargo.content.link))
  )

  // Cargo queue has entries to be aggregated
  const queuedCargoToAggregationBeforeAggregate = await cargoView.select()
  if (!queuedCargoToAggregationBeforeAggregate.ok) {
    throw new Error('no queued cargo to aggregate')
  }
  t.is(queuedCargoToAggregationBeforeAggregate.ok.length, cargoItems.length)

  // Create aggregate with queued items
  const aggregatePiece = {
    link: cargoItems[0].piece.link,
    size: cargoItems[0].piece.size
  }
  const pieces = queuedCargoToAggregationBeforeAggregate.ok.map(cargo => cargo.piece)

  const res = await Promise.all([
    aggregateTable.insert(aggregatePiece, pieces),
    aggregateTable.insert(aggregatePiece, pieces)
  ])
  const okResponses = res.filter(r => r.ok)
  const errorResponses = res.filter(r => r.error)

  t.is(okResponses.length, 1)
  t.is(errorResponses.length, 1)
  t.is(errorResponses[0].error?.name, DatabaseUniqueValueConstraintErrorName)
})

test('concurrent aggregate insertion fail to be added if partially same cargo', async t => {
  const { dbClient } = t.context
  const aggregateTable = useAggregateTable(dbClient)
  const contentTable = useContentTable(dbClient)
  const pieceTable = usePieceTable(dbClient)
  const cargoView = useCargoView(dbClient)
  const cargoItems = await getCargo(20)

  // Queue content to get piece
  await Promise.all(
    cargoItems.map(cargo => contentTable.insert(cargo.content))
  )
  // Fill in pieces for content entries
  await Promise.all(
    cargoItems.map(cargo => pieceTable.insert(cargo.piece, cargo.content.link))
  )

  // Cargo queue has entries to be aggregated
  const queuedCargoToAggregationBeforeAggregate = await cargoView.select()
  if (!queuedCargoToAggregationBeforeAggregate.ok) {
    throw new Error('no queued cargo to aggregate')
  }
  t.is(queuedCargoToAggregationBeforeAggregate.ok.length, cargoItems.length)

  // Create aggregate with queued items
  const aggregatePiece = {
    link: cargoItems[0].piece.link,
    size: cargoItems[0].piece.size
  }
  const pieces = queuedCargoToAggregationBeforeAggregate.ok.map(cargo => cargo.piece)

  const piecesAggregate1 = pieces.slice(0, 15)
  const piecesAggregate2 = pieces.slice(4)

  const res = await Promise.all([
    aggregateTable.insert(aggregatePiece, piecesAggregate1),
    aggregateTable.insert(aggregatePiece, piecesAggregate2)
  ])
  const okResponses = res.filter(r => r.ok)
  const errorResponses = res.filter(r => r.error)

  t.is(okResponses.length, 1)
  t.is(errorResponses.length, 1)
  t.is(errorResponses[0].error?.name, DatabaseUniqueValueConstraintErrorName)

  const queuedCargoToAggregationAfterAggregate = await cargoView.select()
  if (!queuedCargoToAggregationAfterAggregate.ok) {
    throw new Error('no queued cargo after aggregate')
  }
  t.not(queuedCargoToAggregationAfterAggregate.ok.length, cargoItems.length)
})
