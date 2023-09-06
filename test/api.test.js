import { test } from './helpers/context.js'

import git from 'git-rev-sync'
import pWaitFor from 'p-wait-for'
import delay from 'delay'
import { randomCargo } from '@web3-storage/filecoin-api/test'
import { Aggregator } from '@web3-storage/filecoin-client'

import { decode as bufferDecode } from '../packages/core/src/data/buffer.js'

import { getAggregatorClientConfig } from './helpers/aggregator-client.js'
import { getTableItem, getTableItems } from './helpers/table.js'
import { getBucketItem } from './helpers/bucket.js'
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
    pieces.map(p => pWaitFor(async () => {
      /** @type {import('../packages/core/src/data/piece.js').StoreRecord | undefined} */
      // @ts-expect-error does not automatically infer
      const pieceEntry = await getTableItem(
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

      return Boolean(pieceEntry)
    }, {
      interval: 1e3,
      timeout: 50e3
    }))
  )

  // wait for aggregate-store entry to exist given it is propagated with a queue message
  await delay(15e3)

  console.log(`try to fetch aggregate entries for storefront ${storefront}...`)
  await pWaitFor(async () => {
    const aggregateEntries = await getAggregates(t.context, storefront)
    return Boolean(aggregateEntries)
  }, {
    interval: 5e3,
    timeout: 60e3
  })

  // Validate aggregates have pieces
  const aggregateEntries = await getAggregates(t.context, storefront)
  if (!aggregateEntries) {
    throw new Error('aggregate entries not found')
  }

  // Verify buffer used by aggregate
  const bufferKey = `${aggregateEntries[0].buffer}/${aggregateEntries[0].buffer}`
  const bufferItem = await getBucketItem(
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

// Configure groups

/**
 * @param {import("./helpers/context.js").Context} context
 * @param {string} storefront
 */
async function getAggregates (context, storefront) {
  /** @type {import('../packages/core/src/data/aggregate.js').StoreRecord[] | undefined} */
  // @ts-expect-error does not automatically infer
  const aggregateEntries = await getTableItems(
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

  return aggregateEntries
}
