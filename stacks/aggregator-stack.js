import { Queue, use } from 'sst/constructs'
import { Duration } from 'aws-cdk-lib'
import { StartingPosition } from 'aws-cdk-lib/aws-lambda'

import { DataStack } from './data-stack.js'
import {
  setupSentry,
  getAggregatorEnv,
  getDealerEnv,
  getResourceName,
  getCustomDomain
} from './config.js'

/**
 * @param {import('sst/constructs').StackContext} properties
 */
export function AggregatorStack({ stack, app }) {
  const {
    AGGREGATOR_HOSTED_ZONE,
    AGGREGATOR_DID,
    MAX_AGGREGATE_SIZE,
    MIN_AGGREGATE_SIZE,
    MIN_UTILIZATION_FACTOR,
    AGGREGATOR_PROOF
  } = getAggregatorEnv(stack)
  const {
    DEAL_API_HOSTED_ZONE,
    DEALER_DID,
    DEALER_PROOF
  } = getDealerEnv()
  const aggregatorApiCustomDomain = getCustomDomain(stack.stage, AGGREGATOR_HOSTED_ZONE)
  const dealerApiCustomDomain = getCustomDomain(stack.stage, DEAL_API_HOSTED_ZONE)


  // Setup app monitoring with Sentry
  setupSentry(app, stack)

  const {
    aggregatorPieceStoreTable,
    aggregatorBufferStoreBucket,
    aggregatorAggregateStoreTable,
    aggregatorInclusionStoreTable,
    aggregatorInclusionProofStoreBucket,
    aggregatorPrivateKey
  } = use(DataStack)
 
   /**
    * 1st processor queue - piece/offer invocation
    */
   const pieceQueueName = getResourceName('piece-queue', stack.stage)
   const pieceQueueDLQ = new Queue(stack, `${pieceQueueName}-dlq`, {
    cdk: { queue: { retentionPeriod: Duration.days(14) } }
   })
   const pieceQueue = new Queue(stack, pieceQueueName)

  /**
   * 2nd processor queue - buffer reducing event
   */
  const bufferQueueName = getResourceName('buffer-queue', stack.stage)
  const bufferQueueDLQ = new Queue(stack, `${bufferQueueName}-dlq`, {
    cdk: { queue: { retentionPeriod: Duration.days(14) } }
   })
  const bufferQueue = new Queue(stack, bufferQueueName, {
    cdk: {
      queue: {
        // Guarantee exactly-once processing
        // (Note: maximum 10 batch)
        fifo: true,
        // During the deduplication interval (12 minutes), Amazon SQS treats
        // messages that are sent with identical body content
        contentBasedDeduplication: true,
        queueName: `${bufferQueueName}.fifo`,
        visibilityTimeout: Duration.minutes(12)
      }
    },
  })

  /**
   * 3rd processor queue - aggregator/offer invocation
   */
  const aggregateOfferQueueName = getResourceName('aggregate-offer-queue', stack.stage)
  const aggregateOfferQueueDLQ = new Queue(stack, `${aggregateOfferQueueName}-dlq`, {
    cdk: { queue: { retentionPeriod: Duration.days(14) } }
   })
  const aggregateOfferQueue = new Queue(stack, aggregateOfferQueueName, {
    cdk: {
      queue: {
        // Guarantee exactly-once processing
        // (Note: maximum 10 batch)
        fifo: true,
        // During the deduplication interval (5 minutes), Amazon SQS treats
        // messages that are sent with identical body content
        contentBasedDeduplication: true,
        queueName: `${aggregateOfferQueueName}.fifo`
      }
    }
  })

  /**
   * 4th processor queue - piece/accept invocation
   */
  const pieceAcceptQueueName = getResourceName('piece-accept-queue', stack.stage)
  const pieceAcceptQueueDLQ = new Queue(stack, `${pieceAcceptQueueName}-dlq`, {
    cdk: { queue: { retentionPeriod: Duration.days(14) } }
   })
  const pieceAcceptQueue = new Queue(stack, pieceAcceptQueueName)

  /**
   * On piece queue messages, store piece.
   */
  pieceQueue.addConsumer(stack, {
    function: {
      handler: 'packages/functions/src/aggregator/handle-piece-message.main',
      bind: [
        aggregatorPieceStoreTable
      ]
    },
    deadLetterQueue: pieceQueueDLQ.cdk.queue,
    cdk: {
      eventSource: {
        batchSize: 1
      },
    },
  })

  const aggregatorPieceStoreHandleInsertDLQ = new Queue(stack, `aggregator-piece-store-handle-insert-dlq`, {
    cdk: { queue: { retentionPeriod: Duration.days(14) } }
  })
  /**
   * On Piece store insert batch, buffer pieces together to resume buffer processing.
   */
  aggregatorPieceStoreTable.addConsumers(stack, {
    handlePiecesInsert: {
      function: {
        handler: 'packages/functions/src/aggregator/handle-pieces-insert.main',
        environment: {
          BUFFER_STORE_BUCKET_NAME: aggregatorBufferStoreBucket.bucketName,
          BUFFER_QUEUE_URL: bufferQueue.queueUrl,
        },
        permissions: [
          aggregatorBufferStoreBucket,
          bufferQueue
        ],
        timeout: '20 seconds'
      },
      deadLetterQueue: aggregatorPieceStoreHandleInsertDLQ.cdk.queue,
      cdk: {
        // https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_lambda_event_sources.DynamoEventSourceProps.html#filters
        eventSource: {
          batchSize: stack.stage === 'production' ?
            10_000 // Production max out batch size
            : 10, // Integration tests
          maxBatchingWindow: stack.stage === 'production' ?
            Duration.minutes(5) // Production max out batch write to dynamo
            : Duration.seconds(5), // Integration tests
          // allow reporting partial failures
          reportBatchItemFailures: true,
          // Start reading at the last untrimmed record in the shard in the system.
          startingPosition: StartingPosition.TRIM_HORIZON,
        }
      },
      filters: [
        {
          eventName: ['INSERT'],
        }
      ]
    }
  })

  /**
   * On buffer queue messages, reduce received buffer records into a bigger buffer.
   * - If new buffer does not have enough load to build an aggregate, it is stored
   * and requeued for buffer reducing
   * - If new buffer has enough load to build an aggregate, it is stored and queued
   * into aggregateOfferQueue. Remaining of the new buffer (in case buffer bigger
   * than maximum aggregate size) is re-queued into the buffer queue.
   */
  bufferQueue.addConsumer(stack, {
    function: {
      handler: 'packages/functions/src/aggregator/handle-buffer-queue-message.main',
      environment: {
        BUFFER_QUEUE_URL: bufferQueue.queueUrl,
        BUFFER_STORE_BUCKET_NAME: aggregatorBufferStoreBucket.bucketName,
        AGGREGATE_OFFER_QUEUE_URL: aggregateOfferQueue.queueUrl,
        MAX_AGGREGATE_SIZE,
        MIN_AGGREGATE_SIZE,
        MIN_UTILIZATION_FACTOR,
      },
      permissions: [
        bufferQueue,
        aggregatorBufferStoreBucket,
        aggregateOfferQueue
      ],
      timeout: '12 minutes',
      memorySize: '6 GB'
    },
    deadLetterQueue: bufferQueueDLQ.cdk.queue,
    cdk: {
      eventSource: {
        // we can reduce most buffers possible at same time to avoid large buffers to create huge congestion on the queue while being processed.
        // also makes fewer lambda calls and decreases overall execution time.
        batchSize: 2,
        // allow reporting partial failures
        reportBatchItemFailures: true,
      },
    },
  })

  /**
   * On aggregate offer queue message, store aggregate record in store.
   */
  aggregateOfferQueue.addConsumer(stack, {
    function: {
      handler: 'packages/functions/src/aggregator/handle-aggregate-offer-message.main',
      bind: [
        aggregatorAggregateStoreTable
      ],
    },
    deadLetterQueue: aggregateOfferQueueDLQ.cdk.queue,
    cdk: {
      eventSource: {
        batchSize: 1,
      },
    }
  })

  const aggregatorAggregateStoreHandleInsertToPieceAcceptDLQ = new Queue(stack, `aggregate-store-handle-piece-accept-dlq`, {
    cdk: { queue: { retentionPeriod: Duration.days(14) } }
  })
  const aggregatorAggregateStoreHandleInsertToAggregateOfferDLQ = new Queue(stack, `aggregate-store-handle-aggregate-offer-dlq`, {
    cdk: { queue: { retentionPeriod: Duration.days(14) } }
  })
  aggregatorAggregateStoreTable.addConsumers(stack, {
    /**
     * On Aggregate store insert, offer inserted aggregate for deal.
     */
    handleAggregateInsertToPieceAcceptQueue: {
      function: {
        handler: 'packages/functions/src/aggregator/handle-aggregate-insert-to-piece-accept-queue.main',
        environment: {
          BUFFER_STORE_BUCKET_NAME: aggregatorBufferStoreBucket.bucketName,
          PIECE_ACCEPT_QUEUE_URL: pieceAcceptQueue.queueUrl,
          MAX_AGGREGATE_SIZE,
          MIN_AGGREGATE_SIZE,
          MIN_UTILIZATION_FACTOR,
        },
        permissions: [
          aggregatorBufferStoreBucket,
          pieceAcceptQueue
        ],
        timeout: '5 minutes'
      },
      deadLetterQueue: aggregatorAggregateStoreHandleInsertToPieceAcceptDLQ.cdk.queue,
      cdk: {
        eventSource: {
          batchSize: 1,
          // Start reading at the last untrimmed record in the shard in the system.
          startingPosition: StartingPosition.TRIM_HORIZON,
        },
      },
      filters: [
        {
          eventName: ['INSERT'],
        }
      ]
    },
    handleAggregateInsertToAggregateOffer: {
      function: {
        handler: 'packages/functions/src/aggregator/handle-aggregate-insert-to-aggregate-offer.main',
        environment: {
          BUFFER_STORE_BUCKET_NAME: aggregatorBufferStoreBucket.bucketName,
          DID: AGGREGATOR_DID,
          SERVICE_DID: DEALER_DID,
          SERVICE_URL: dealerApiCustomDomain?.domainName ? `https://${dealerApiCustomDomain?.domainName}` : '',
          PROOF: DEALER_PROOF
        },
        permissions: [
          aggregatorBufferStoreBucket,
        ],
        bind: [
          aggregatorPrivateKey
        ]
      },
      deadLetterQueue: aggregatorAggregateStoreHandleInsertToAggregateOfferDLQ.cdk.queue,
      cdk: {
        eventSource: {
          batchSize: 1,
          // Start reading at the last untrimmed record in the shard in the system.
          startingPosition: StartingPosition.TRIM_HORIZON,
        },
      },
      filters: [
        {
          eventName: ['INSERT'],
        }
      ]
    }
  })

  /**
   * On piece accept queue message, store inclusion record in store.
   */
  pieceAcceptQueue.addConsumer(stack, {
    function: {
      handler: 'packages/functions/src/aggregator/handle-piece-accept-message.main',
      environment: {
        INCLUSION_PROOF_STORE_BUCKET_NAME: aggregatorInclusionProofStoreBucket.bucketName,
      },
      permissions: [
        aggregatorInclusionProofStoreBucket,
      ],
      bind: [
        aggregatorInclusionStoreTable,
      ],
      timeout: '30 seconds'
    },
    deadLetterQueue: pieceAcceptQueueDLQ.cdk.queue,
    cdk: {
      eventSource: {
        batchSize: 1,
      },
    }
  })

  const aggregatorInclusionStoreHandleInsertToUpdateStateDLQ = new Queue(stack, `inclusion-store-handle-update-state-dlq`, {
    cdk: { queue: { retentionPeriod: Duration.days(14) } }
  })
  const aggregatorInclusionStoreHandleInsertToPieceAcceptDLQ = new Queue(stack, `inclusion-store-handle-piece-accept-dlq`, {
    cdk: { queue: { retentionPeriod: Duration.days(14) } }
  })
  aggregatorInclusionStoreTable.addConsumers(stack, {
    /**
     * On Inclusion store insert, piece table can be updated to reflect piece state.
     */
    handleInclusionInsertToUpdateState: {
      function: {
        handler: 'packages/functions/src/aggregator/handle-inclusion-insert-to-update-state.main',
        environment: {},
        bind: [
          aggregatorPieceStoreTable,
        ]
      },
      deadLetterQueue: aggregatorInclusionStoreHandleInsertToUpdateStateDLQ.cdk.queue,
      cdk: {
        eventSource: {
          batchSize: 1,
          // Start reading at the last untrimmed record in the shard in the system.
          startingPosition: StartingPosition.TRIM_HORIZON,
        },
      },
      filters: [
        {
          eventName: ['INSERT'],
        }
      ]
    },
    /**
     * On Inclusion store insert, invoke piece/accept.
     */
    handleInclusionInsertToIssuePieceAccept: {
      function: {
        handler: 'packages/functions/src/aggregator/handle-inclusion-insert-to-issue-piece-accept.main',
        environment: {
          DID: AGGREGATOR_DID,
          SERVICE_DID: AGGREGATOR_DID,
          SERVICE_URL: aggregatorApiCustomDomain?.domainName ? `https://${aggregatorApiCustomDomain?.domainName}` : '',
          PROOF: AGGREGATOR_PROOF,
        },
        bind: [
          aggregatorPrivateKey
        ],
        timeout: '30 seconds'
      },
      deadLetterQueue: aggregatorInclusionStoreHandleInsertToPieceAcceptDLQ.cdk.queue,
      cdk: {
        eventSource: {
          batchSize: 1,
          // Start reading at the last untrimmed record in the shard in the system.
          startingPosition: StartingPosition.TRIM_HORIZON,
        },
      },
      filters: [
        {
          eventName: ['INSERT'],
        }
      ]
    }
  })

  return {
    pieceQueue,
    bufferQueue,
    aggregateOfferQueue,
    pieceAcceptQueue
  }
}
