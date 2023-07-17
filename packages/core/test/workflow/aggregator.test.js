import { testWorkflow as test } from '../helpers/context.js'

import { Aggregate, Piece } from '@web3-storage/data-segment'

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
  const {
    pieceQueue,
    aggregateQueue,
  } = await prepare(dbClient)

  const { error, ok } = await aggregatorWorkflow.run({
    pieceQueue,
    aggregateQueue,
  })
  t.falsy(error)
  t.is(ok?.count, 0)
})

test('can consume piece queue with content to create aggregate', async t => {
  const { dbClient } = t.context
  const {
    contentQueue,
    pieceQueue,
    aggregateQueue,
  } = await prepare(dbClient)
  const builderSize = Piece.PaddedSize.from(1 << 20)
  await addFixtures({ contentQueue, pieceQueue, builderSize })

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
  t.truthy(queuedAggregatesResponse.ok?.[0].inserted)

  // Validate aggregate
  t.truthy(queuedAggregatesResponse.ok?.[0].link)
  t.deepEqual(queuedAggregatesResponse.ok?.[0].size, builderSize)
})

/**
 * @param {import('kysely').Kysely<import('../../src/schema.js').Database>} dbClient
 */
async function prepare (dbClient) {
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const aggregateQueue = createAggregateQueue(dbClient)

  return {
    contentQueue,
    pieceQueue,
    aggregateQueue,
  }
}

/**
 * @param {object} props
 * @param {import('../../src/types').ContentQueue} props.contentQueue
 * @param {import('../../src/types').PieceQueue} props.pieceQueue
 * @param {import('@web3-storage/data-segment').PaddedPieceSize} props.builderSize
 */
async function addFixtures ({ contentQueue, pieceQueue, builderSize }) {
  // Prepare pieces by adding to content queue, then piece queue and then aggregate queue
  const cargoSize = 8
  const cargoItems = (await getCargo(cargoSize))
    .sort((a, b) => Number(a.piece.size - b.piece.size))
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
  const builder = Aggregate.createBuilder({
    size: builderSize
  })
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

  return {
    cargoItems,
    aggregate
  }
}
