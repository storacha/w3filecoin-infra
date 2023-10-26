import {
  Bucket,
  Config,
  Table
} from 'sst/constructs'

import {
  // aggregator
  pieceStoreTableProps,
  aggregateStoreTableProps,
  inclusionStoreTableProps,
  // dealer
  dealerAggregateStoreTableProps,
  // deal-tracker
  dealStoreTableProps
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

  // Secrets
  const privateKey = new Config.Secret(stack, 'PRIVATE_KEY')
  const dealerPrivateKey = new Config.Secret(stack, 'DEALER_PRIVATE_KEY')
  const dealTrackerPrivateKey = new Config.Secret(stack, 'DEAL_TRACKER_PRIVATE_KEY')

  // --------------------------------- Aggregator ---------------------------------
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

  // --------------------------------- Dealer ---------------------------------
  const offerBucket = getBucketConfig('dealer-offer-store', stack.stage)
  const dealerOfferStoreBucket = new Bucket(stack, offerBucket.bucketName, {
    cors: true,
    cdk: {
      bucket: offerBucket
    }
  })

  const dealerAggregateStoreTableName = 'dealer-aggregate-store'
  const dealerAggregateStoreTable = new Table(stack, dealerAggregateStoreTableName, {
    ...dealerAggregateStoreTableProps,
    // information that will be written to the stream
    stream: 'new_and_old_images'
  })

  // --------------------------------- Deal Tracker ---------------------------------
  /**
   * Deal archive store used to store active replicas reported by Spade Oracle.
   */
  const dealTrackerDealArchiveBucket = getBucketConfig('deal-tracker-deal-archive-store', stack.stage)
  const dealTrackerDealArchiveStoreBucket = new Bucket(stack, dealTrackerDealArchiveBucket.bucketName, {
    cors: true,
    cdk: {
      bucket: dealTrackerDealArchiveBucket
    }
  })

  /**
   * Deal store used to store deal information 
   */
  const dealTrackerDealStoreTableName = 'deal-tracker-deal-store'
  const dealTrackerDealStoreTable = new Table(stack, dealTrackerDealStoreTableName, {
    ...dealStoreTableProps,
    // information that will be written to the stream
    stream: 'new_and_old_images'
  })

  stack.addOutputs({
    // Aggregator
    BufferBucketName: bucket.bucketName,
    PieceTableName: pieceStoreTableName,
    AggregateTableName: aggregateStoreTableName,
    InclusionTableName: inclusionStoreTableName,
    // Deal Tracker
    DealTrackerDealArchiveBucketName: dealTrackerDealArchiveStoreBucket.bucketName,
    DealTrackerDealStoreTableName: dealTrackerDealStoreTableName
  })

  return {
    // secrets
    privateKey,
    dealerPrivateKey,
    dealTrackerPrivateKey,
    // aggregator stores
    bufferStoreBucket,
    pieceStoreTable,
    aggregateStoreTable,
    inclusionStoreTable,
    // dealer stores
    dealerOfferStoreBucket,
    dealerAggregateStoreTable,
    // deal tracker stores
    dealTrackerDealArchiveStoreBucket,
    dealTrackerDealStoreTable
  }
}
