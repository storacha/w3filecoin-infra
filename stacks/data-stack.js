import {
  Bucket,
  Config,
  Table
} from 'sst/constructs'

import {
  // aggregator
  aggregatorPieceStoreTableProps,
  aggregatorAggregateStoreTableProps,
  aggregatorInclusionStoreTableProps,
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
  const aggregatorPrivateKey = new Config.Secret(stack, 'AGGREGATOR_PRIVATE_KEY')
  const dealerPrivateKey = new Config.Secret(stack, 'DEALER_PRIVATE_KEY')
  const dealTrackerPrivateKey = new Config.Secret(stack, 'DEAL_TRACKER_PRIVATE_KEY')

  // --------------------------------- Aggregator ---------------------------------
  /**
   * Buffer store used to buffer multiple pieces consumed from first workflow (`piece-queue`),
   * concatenating them on the second workflow (`buffer-queue`) until a buffer can be used
   * to create an aggregate with desired size.
   */
  const aggregatorBufferBucket = getBucketConfig('aggregator-buffer-store', stack.stage)
  const aggregatorBufferStoreBucket = new Bucket(stack, aggregatorBufferBucket.bucketName, {
    cors: true,
    cdk: {
      bucket: aggregatorBufferBucket
    }
  })

  /**
   * Piece store used to guarantee write uniqueness for first workflow (`piece-queue`).
   * It records all items that get into the workflow pipeline.
   */
  const aggregatorPieceStoreTableName = 'aggregator-piece-store'
  const aggregatorPieceStoreTable = new Table(stack, aggregatorPieceStoreTableName, {
    ...aggregatorPieceStoreTableProps,
    // information that will be written to the stream
    stream: 'new_and_old_images'
  })

  /**
   * Aggregate store used by the third workflow (`aggregate-queue`).
   * It records all aggregate items offered to broker.
   */
  const aggregatorAggregateStoreTableName = 'aggregator-aggregate-store'
  const aggregatorAggregateStoreTable = new Table(stack, aggregatorAggregateStoreTableName, {
    ...aggregatorAggregateStoreTableProps,
    // information that will be written to the stream
    stream: 'new_and_old_images'
  })

  /**
   * Inclusion stored used by cron workflow (`deal-tracker`).
   * It records all inclusion records when we have a resolved deal for an aggregate.
   */
  const aggregatorInclusionStoreTableName = 'aggregator-inclusion-store'
  const aggregatorInclusionStoreTable = new Table(stack, aggregatorInclusionStoreTableName, {
    ...aggregatorInclusionStoreTableProps,
    // information that will be written to the stream
    stream: 'new_and_old_images'
  })

  /**
   * Buffer store used to buffer multiple pieces consumed from first workflow (`piece-queue`),
   * concatenating them on the second workflow (`buffer-queue`) until a buffer can be used
   * to create an aggregate with desired size.
   */
  const aggregatorInclusionProofBucket = getBucketConfig('aggregator-inclusion-proof-store', stack.stage)
  const aggregatorInclusionProofStoreBucket = new Bucket(stack, aggregatorInclusionProofBucket.bucketName, {
    cors: true,
    cdk: {
      bucket: aggregatorInclusionProofBucket
    }
  })

  // --------------------------------- Dealer ---------------------------------
  const dealerOfferBucket = getBucketConfig('dealer-offer-store', stack.stage)
  const dealerOfferStoreBucket = new Bucket(stack, dealerOfferBucket.bucketName, {
    cors: true,
    cdk: {
      bucket: dealerOfferBucket
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
    AggregatorBufferBucketName: aggregatorBufferBucket.bucketName,
    AggregatorPieceTableName: aggregatorPieceStoreTable.tableName,
    AggregatorAggregateTableName: aggregatorAggregateStoreTable.tableName,
    AggregatorInclusionTableName: aggregatorInclusionStoreTable.tableName,
    AggregatorInclusionProofBucketName: aggregatorInclusionProofBucket.bucketName,
    // Dealer
    DealerOfferStoreBucketName: dealerOfferBucket.bucketName,
    DealerAggregateStoreTableName: dealerAggregateStoreTable.tableName,
    // Deal Tracker
    DealTrackerDealArchiveBucketName: dealTrackerDealArchiveStoreBucket.bucketName,
    DealTrackerDealStoreTableName: dealTrackerDealStoreTable.tableName
  })

  return {
    // secrets
    aggregatorPrivateKey,
    dealerPrivateKey,
    dealTrackerPrivateKey,
    // aggregator stores
    aggregatorBufferStoreBucket,
    aggregatorPieceStoreTable,
    aggregatorAggregateStoreTable,
    aggregatorInclusionStoreTable,
    aggregatorInclusionProofStoreBucket,
    // dealer stores
    dealerOfferStoreBucket,
    dealerAggregateStoreTable,
    // deal tracker stores
    dealTrackerDealArchiveStoreBucket,
    dealTrackerDealStoreTable
  }
}
