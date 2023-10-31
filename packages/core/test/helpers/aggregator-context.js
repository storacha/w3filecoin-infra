// table props
import {
  aggregatorPieceStoreTableProps,
  aggregatorAggregateStoreTableProps,
  aggregatorInclusionStoreTableProps
} from '../../src/store/index.js'

// store clients
import { createClient as createPieceStoreClient } from '../../src/store/aggregator-piece-store.js'
import { createClient as createBufferStoreClient } from '../../src/store/aggregator-buffer-store.js'
import { createClient as createInclusionProofStoreClient } from '../../src/store/aggregator-inclusion-proof-store.js'
import { createClient as createInclusionStoreClient } from '../../src/store/aggregator-inclusion-store.js'
import { createClient as createAggregateStoreClient } from '../../src/store/aggregator-aggregate-store.js'

// queue clients
import { createClient as createPieceQueueClient } from '../../src/queue/piece-queue.js'
import { createClient as createBufferQueueClient } from '../../src/queue/buffer-queue.js'
import { createClient as createAggregateOfferQueueClient } from '../../src/queue/aggregate-offer-queue.js'
import { createClient as createPieceAcceptQueueClient } from '../../src/queue/piece-accept-queue.js'

import { createTable, createBucket } from './resources.js'

/**
 * @param {import('./context.js').BucketContext & import('./context.js').DbContext} ctx
 */
export async function getStores (ctx) {
  const { dynamoClient, s3 } = ctx
  const [
    bufferStoreBucketName,
    inclusionProofStoreBucketName
  ] = await Promise.all([
    createBucket(s3),
    createBucket(s3)
  ])
  const [
    pieceStoreTableName,
    aggregateStoreTableName,
    inclusionStoreTableName
  ] = await Promise.all([
    createTable(dynamoClient, aggregatorPieceStoreTableProps),
    createTable(dynamoClient, aggregatorAggregateStoreTableProps),
    createTable(dynamoClient, aggregatorInclusionStoreTableProps),
  ])

  return {
    aggregateStore: createAggregateStoreClient(dynamoClient, { tableName: aggregateStoreTableName }),
    bufferStore: createBufferStoreClient(s3, { name: bufferStoreBucketName }),
    inclusionStore: createInclusionStoreClient(
      dynamoClient,
      {
        tableName: inclusionStoreTableName,
        inclusionProofStore: createInclusionProofStoreClient(s3, { name: inclusionProofStoreBucketName })
      }
    ),
    pieceStore: createPieceStoreClient(dynamoClient, { tableName: pieceStoreTableName }),
  }
}

/**
 * @param {import('./context.js').MultipleQueueContext} ctx
 */
export function getQueues (ctx) {
  return {
    pieceQueue: createPieceQueueClient(ctx.queues.pieceQueue.sqsClient,
      { queueUrl: ctx.queues.pieceQueue.queueUrl }
    ),
    bufferQueue: createBufferQueueClient(ctx.queues.bufferQueue.sqsClient,
      {
        queueUrl: ctx.queues.bufferQueue.queueUrl,
        // testing is not FIFO QUEUE
        disableMessageGroupId: true
      }
    ),
    aggregateOfferQueue: createAggregateOfferQueueClient(ctx.queues.aggregateOfferQueue.sqsClient,
      {
        queueUrl: ctx.queues.aggregateOfferQueue.queueUrl,
        // testing is not FIFO QUEUE
        disableMessageGroupId: true
      }
    ),
    pieceAcceptQueue: createPieceAcceptQueueClient(ctx.queues.pieceAcceptQueue.sqsClient,
      { queueUrl: ctx.queues.pieceAcceptQueue.queueUrl }
    ),
  }
}
