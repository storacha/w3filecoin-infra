import { testWorkflow as test } from '../helpers/context.js'

import { Piece } from '@web3-storage/data-segment'

import { createContentQueue } from '../../src/queue/content.js'
import { createPieceQueue } from '../../src/queue/piece.js'
import { createAggregateQueue } from '../../src/queue/aggregate.js'
import * as aggregatorWorkflow from '../../src/workflow/aggregator.js'

import { createDatabase } from '../helpers/resources.js'
import { getCargo } from '../helpers/cargo.js'

test.beforeEach(async t => {
  Object.assign(t.context, {
    dbClient: (await createDatabase()).client,
  })
})

test('can consume empty piece queue', async t => {
  const { dbClient } = t.context
  const pieceQueue = createPieceQueue(dbClient)
  const aggregateQueue = createAggregateQueue(dbClient)

  const { error, ok } = await aggregatorWorkflow.run({
    pieceQueue,
    aggregateQueue,
  })
  t.falsy(error)
  t.is(ok?.count, 0)
})

test('can consume piece queue with content to create aggregate', async t => {
  const { dbClient } = t.context
  const builderSize = Piece.PaddedSize.from(1 << 20)
  const cargoSize = 8
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const aggregateQueue = createAggregateQueue(dbClient)

  // Prepare pieces by adding to content queue and then piece queue
  const cargoItems = await getCargo(cargoSize)
  const contentQueuePutResp = await Promise.all(
    cargoItems.map(item => contentQueue.put(item.content))
  )
  t.falsy(contentQueuePutResp.find(resp => resp.error))
  const pieceQueuePutResp = await Promise.all(cargoItems.map(item => pieceQueue.put({
    link: item.piece.link,
    // Write bumped size for testing purposes
    size: item.piece.size,
    content: item.content.link
  })))
  t.falsy(pieceQueuePutResp.find(resp => resp.error))

  // Attempt to build aggregate
  const { error, ok } = await aggregatorWorkflow.run({
    pieceQueue,
    aggregateQueue,
    builderSize
  })
  t.falsy(error)
  t.is(ok?.count, 1)

  // Aggregate queue has entries to be offered
  const queuedAggregatesResponse = await aggregateQueue.peek()
  t.falsy(queuedAggregatesResponse.error)

  t.is(queuedAggregatesResponse.ok?.length, 1)
  t.truthy(queuedAggregatesResponse.ok?.[0].link)
  t.truthy(queuedAggregatesResponse.ok?.[0].inserted)
  t.deepEqual(queuedAggregatesResponse.ok?.[0].size, builderSize)
})
