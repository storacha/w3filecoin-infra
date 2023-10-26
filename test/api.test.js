import { test } from './helpers/context.js'

import git from 'git-rev-sync'
import pRetry from 'p-retry'
import delay from 'delay'
import { randomCargo } from '@web3-storage/filecoin-api-legacy/test'
import { Aggregator } from '@web3-storage/filecoin-client-legacy'

import { decode as bufferDecode } from '../packages/core/src/data/buffer.js'

import { getAggregatorClientConfig } from './helpers/aggregator-client.js'
import { waitForTableItem, waitForTableItems } from './helpers/table.js'
import { waitForBucketItem } from './helpers/bucket.js'
import {
  getApiEndpoint,
  getStage,
  getPieceStoreDynamoDb,
  getAggregateStoreDynamoDb,
  getBufferStoreBucketInfo,
} from './helpers/deployment.js'

test.before(t => {
  t.context = {
    apiEndpoint: getApiEndpoint(),
    pieceStoreDynamo: getPieceStoreDynamoDb(),
    aggregateStoreDynamo: getAggregateStoreDynamoDb(),
    bufferStoreBucket: getBufferStoreBucketInfo()
  }
})

test('GET /version', async t => {
  const stage = getStage()
  const response = await fetch(`${t.context.apiEndpoint}/version`)
  t.is(response.status, 200)

  const body = await response.json()
  t.is(body.env, stage)
  t.is(body.commit, git.long('.'))
})

test('POST /', async t => {
  const {
    invocationConfig,
    connection
  } = await getAggregatorClientConfig(new URL(t.context.apiEndpoint))
  const storefront = invocationConfig.with

  // Create random pieces to add
  const pieces = await randomCargo(10, 1024)

  // Queue all pieces to be added
  const aggregateQueueResponses = await Promise.all(
    pieces.map(p => Aggregator.aggregateQueue(
      invocationConfig,
      p.link, // put one piece
      storefront,
      // @ts-expect-error multiple versions of ucanto, will change once we drop old aggregator code
      { connection }
    ))
  )

  // All pieces succeeded to be queued
  t.is(
    aggregateQueueResponses.reduce((accum, res) => {
      if (res.out.ok) {
        accum += 1
      }
      return accum
    }, 0),
    pieces.length
  )

  // wait for piece-store entry to exist given it is propagated with a queue message to be added
  await delay(5e3)

  console.log('try to fetch piece entries...')
  await Promise.all(
    pieces.map(p => pRetry(async () => {
      /** @type {import('../packages/core/src/data/piece.js').StoreRecord | undefined} */
      // @ts-expect-error does not automatically infer
      const pieceEntry = await waitForTableItem(
        t.context.pieceStoreDynamo.client,
        t.context.pieceStoreDynamo.tableName,
        { piece: p.link.toString(), storefront: invocationConfig.with }
      )
      // Validate piece entry content
      if (pieceEntry) {
        t.is(pieceEntry.piece, p.link.toString())
        t.is(pieceEntry.storefront, invocationConfig.with)
        t.is(pieceEntry.group, invocationConfig.with)
      }

      return pieceEntry
    }))
  )

  // wait for aggregate-store entry to exist given it is propagated with a queue message
  await delay(25e3)

  console.log(`try to fetch aggregate entries for storefront ${storefront}...`)
  // Validate aggregates have pieces
  const aggregateEntries = await pRetry(() => getAggregates(t.context, storefront), {
    minTimeout: 3e3,
    retries: 1e3
  })
  if (!aggregateEntries) {
    throw new Error('aggregate entries not found')
  }

  // Verify buffer used by aggregate
  const bufferKey = `${aggregateEntries[0].buffer}/${aggregateEntries[0].buffer}`
  const bufferItem = await waitForBucketItem(
    t.context.bufferStoreBucket.client,
    t.context.bufferStoreBucket.bucket,
    bufferKey
  )
  if (!bufferItem) {
    throw new Error('offer store item was not found')
  }
  // verify buffer pieces are in added pieces
  const buffer = await bufferDecode.storeRecord({
    key: bufferKey,
    value: bufferItem
  })
  for (const bufferPiece of buffer.pieces) {
    t.truthy(
      pieces.find(p => p.link.equals(bufferPiece.piece))
    )
  }
})

/**
 * @param {import('./helpers/context.js').Context} context
 * @param {string} storefront
 */
async function getAggregates (context, storefront) {
  /** @type {import('../packages/core/src/data/aggregate.js').StoreRecord[] | undefined} */
  // @ts-expect-error does not automatically infer
  const aggregateEntries = await waitForTableItems(
    context.aggregateStoreDynamo.client,
    context.aggregateStoreDynamo.tableName,
    {
      storefront: {
        ComparisonOperator: 'EQ',
        AttributeValueList: [{ S: storefront }]
      }
    },
    {
      indexName: 'indexStorefront'
    }
  )

  if (!aggregateEntries || !aggregateEntries.length) {
    console.log(`aggregates for given storefront are still not available`)
    throw new Error('aggregates not found for given storefront')
  }

  return aggregateEntries
}
