import {
  Bucket,
  Table
} from 'sst/constructs'

import {
  pieceStoreTableProps,
  aggregateStoreTableProps,
  inclusionStoreTableProps
} from '../packages/core/src/store'

import {
  setupSentry,
  getBucketConfig
} from './config.js'

/**
 * @param {import('sst/constructs').StackContext} properties
 */
export function DataStack({ stack, app }) {
  // Setup app monitoring with Sentry
  setupSentry(app, stack)

  /**
   * Buffer store used to buffer multiple pieces consumed from first workflow (`piece-queue`),
   * concatenating them on the second workflow (`buffer-queue`) until a buffer can be used
   * to create an aggregate with desired size.
   */
  const bucket = getBucketConfig('buffer-store', stack.stage)
  const bufferStoreBucket = new Bucket(stack, bucket.bucketName, {
    cors: true,
    cdk: {
      bucket
    }
  })

  /**
   * Piece store used to guarantee write uniqueness for first workflow (`piece-queue`).
   * It records all items that get into the workflow pipeline.
   */
  const pieceStoreTable = new Table(stack, 'piece-store', pieceStoreTableProps)

  /**
   * Aggregate store used by the third workflow (`aggregate-queue`).
   * It records all aggregate items offered to broker.
   */
  const aggregateStoreTable = new Table(stack, 'aggregate-store', aggregateStoreTableProps)

  /**
   * Inclusion stored used by cron workflow (`deal-tracker`).
   * It records all inclusion records when we have a resolved deal for an aggregate.
   */
  const inclusionStoreTable = new Table(stack, 'inclusion-store', inclusionStoreTableProps)

  return {
    bufferStoreBucket,
    pieceStoreTable,
    aggregateStoreTable,
    inclusionStoreTable
  }
}
