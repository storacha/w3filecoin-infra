import {
  Bucket,
  Table
} from 'sst/constructs'

import {
  pieceStoreTableProps,
  aggregateStoreTableProps,
  inclusionStoreTableProps
} from '../packages/core/src/store/index.js'

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
  const pieceStoreTableName = 'piece-store'
  const pieceStoreTable = new Table(stack, pieceStoreTableName, {
    // TODO: Expire testing table entries
    // https://dynobase.dev/dynamodb-ttl/
    // https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_dynamodb.Table.html#timetoliveattribute
    ...pieceStoreTableProps
  })

  /**
   * Aggregate store used by the third workflow (`aggregate-queue`).
   * It records all aggregate items offered to broker.
   */
  const aggregateStoreTableName = 'aggregate-store'
  const aggregateStoreTable = new Table(stack, aggregateStoreTableName, aggregateStoreTableProps)

  /**
   * Inclusion stored used by cron workflow (`deal-tracker`).
   * It records all inclusion records when we have a resolved deal for an aggregate.
   */
  const inclusionStoreTableName = 'inclusion-store'
  const inclusionStoreTable = new Table(stack, inclusionStoreTableName, inclusionStoreTableProps)

  stack.addOutputs({
    BufferBucketName: bucket.bucketName,
    PieceTableName: pieceStoreTableName,
    AggregateTableName: aggregateStoreTableName,
    InclusionTableName: inclusionStoreTableName,
  })

  return {
    bufferStoreBucket,
    pieceStoreTable,
    aggregateStoreTable,
    inclusionStoreTable
  }
}
