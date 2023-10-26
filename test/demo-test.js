import { getAggregatorClientConfig } from './helpers/aggregator-client.js'
import { randomCargo } from '@web3-storage/filecoin-api/test'
import { Aggregator } from '@web3-storage/filecoin-client'

import { decode as bufferDecode } from '../packages/core/src/data/buffer.js'

import pRetry from 'p-retry'
import delay from 'delay'

import { waitForBucketItem } from './helpers/bucket.js'
import { waitForTableItem, waitForTableItems } from './helpers/table.js'
import {
  getPieceStoreDynamoDb,
  getAggregateStoreDynamoDb,
  getBufferStoreBucketInfo,
} from './helpers/deployment.js'

const pieceStoreDynamo = getPieceStoreDynamoDb()
const aggregateStoreDynamo = getAggregateStoreDynamoDb()
const bufferStoreBucket = getBufferStoreBucketInfo()

// Get invocation config and connection to aggregator service
const {
  invocationConfig,
  connection
} = await getAggregatorClientConfig(new URL('https://vcs.filecoin.web3.storage'))
const storefront = invocationConfig.with

// Create random pieces to add
const pieces = await randomCargo(8, 1024)
console.log('generate pieces', pieces.map(p => p.link))

// Queue all pieces to be added
const aggregateQueueResponses = await Promise.all(
  pieces.map(p => Aggregator.aggregateQueue(
    invocationConfig,
    p.link, // put one piece
    storefront,
    { connection }
  ))
)

console.log(`queues pieces for aggregation (aggregate/queue invocation): ${aggregateQueueResponses.reduce((accum, res) => {
  if (res.out.ok) accum += 1
  return accum
}, 0)}`)

// wait for piece-store entry to exist given it is propagated with a queue message to be added
await delay(5e3)

console.log('wait for pieces to get into pipeline (aggregate/add invocation)')
await Promise.all(
  pieces.map(p => pRetry(() => waitForTableItem(
    pieceStoreDynamo.client,
    pieceStoreDynamo.tableName,
    { piece: p.link.toString(), storefront: invocationConfig.with }
  )))
)

// Check pieces table
// https://us-west-2.console.aws.amazon.com/dynamodbv2/home?region=us-west-2#item-explorer?table=vcs-w3filecoin-piece-store

// wait for aggregate-store entry to exist given it is propagated with a queue message
await delay(25e3)

console.log(`try to fetch aggregate entries for storefront ${storefront}...`)
// Validate aggregates have pieces
const aggregateEntries = await pRetry(() => getAggregates(aggregateStoreDynamo.client, aggregateStoreDynamo.tableName, storefront), {
  minTimeout: 3e3,
  retries: 1e3
})
console.log('aggregate entries', aggregateEntries)

// Verify buffer used by aggregate
const bufferKey = `${aggregateEntries[0].buffer}/${aggregateEntries[0].buffer}`
const bufferItem = await waitForBucketItem(
  bufferStoreBucket.client,
  bufferStoreBucket.bucket,
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
console.log('pieces in buffer', buffer.pieces.map(p => p.piece))

// Verify buffers
// https://s3.console.aws.amazon.com/s3/buckets/buffer-store-vcs-0?region=us-west-2&tab=objects

// Verify aggregates stored
// https://us-west-2.console.aws.amazon.com/dynamodbv2/home?region=us-west-2#item-explorer?table=vcs-w3filecoin-aggregate-store

// Verify deal was queued `dealer/queue`
// https://us-west-2.console.aws.amazon.com/dynamodbv2/home?region=us-west-2#item-explorer?table=vcs-dealer-deal-store

// Verify dealer added the aggregate offer `dealer/add`
// https://s3.console.aws.amazon.com/s3/buckets/vcs-dealer-offer-store-0?region=us-west-2&tab=objects

/**
 * @param {import('@aws-sdk/client-dynamodb').DynamoDBClient} client
 * @param {string} tableName
 * @param {string} storefront
 */
async function getAggregates (client, tableName, storefront) {
  /** @type {import('../packages/core/src/data/aggregate.js').StoreRecord[] | undefined} */
  // @ts-expect-error does not automatically infer
  const aggregateEntries = await waitForTableItems(
    client,
    tableName,
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
