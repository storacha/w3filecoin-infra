import { Queue, Config, use } from 'sst/constructs'
import { Duration } from 'aws-cdk-lib'

import { DataStack } from './data-stack.js'
import {
  setupSentry,
  getEnv,
  getResourceName,
  getCustomDomain
} from './config.js'

/**
 * @param {import('sst/constructs').StackContext} properties
 */
export function ProcessorStack({ stack, app }) {
  const {
    DID,
    DEALER_DID,
    DEALER_URL,
    MAX_AGGREGATE_SIZE,
    MIN_AGGREGATE_SIZE,
  } = getEnv(stack)

  // Setup app monitoring with Sentry
  setupSentry(app, stack)

  const privateKey = new Config.Secret(stack, 'PRIVATE_KEY')
  const apiCustomDomain = getCustomDomain(stack.stage, process.env.HOSTED_ZONE)

  const {
    bufferStoreBucket,
    aggregateStoreTable
  } = use(DataStack)

   /**
    * trigger for processor - piece/add self invocation
    */
   const pieceAddQueueName = getResourceName('piece-add-queue', stack.stage)
   const pieceAddQueue = new Queue(stack, pieceAddQueueName)
 
   /**
    * 1st processor queue - piece buffering workflow
    */
   const pieceBufferQueueName = getResourceName('piece-buffer-queue', stack.stage)
   const pieceBufferQueue = new Queue(stack, pieceBufferQueueName)

  /**
   * 2nd processor queue - buffer reducing workflow
   */
  const bufferQueueName = getResourceName('buffer-queue', stack.stage)
  const bufferQueue = new Queue(stack, bufferQueueName, {
    cdk: {
      queue: {
        // Guarantee exactly-once processing
        // (Note: maximum 10 batch)
        fifo: true,
        // During the deduplication interval (5 minutes), Amazon SQS treats
        // messages that are sent with identical body content
        contentBasedDeduplication: true,
        queueName: `${bufferQueueName}.fifo`
      }
    }
  })

  /**
   * 3nd processor queue - aggregator workflow
   */
  const aggregateQueueName = getResourceName('aggregate-queue', stack.stage)
  const aggregateQueue = new Queue(stack, aggregateQueueName, {
    cdk: {
      queue: {
        // Guarantee exactly-once processing
        // (Note: maximum 10 batch)
        fifo: true,
        // During the deduplication interval (5 minutes), Amazon SQS treats
        // messages that are sent with identical body content
        contentBasedDeduplication: true,
        queueName: `${aggregateQueueName}.fifo`
      }
    }
  })

  /**
   * Handle queued pieces received from multiple producers by adding them for buffering
   */
  pieceAddQueue.addConsumer(stack, {
    function: {
      handler: 'packages/functions/src/processor/piece-add.workflow',
      environment: {
        AGGREGATOR_DID: process.env.DID ?? '',
        AGGREGATOR_URL: apiCustomDomain?.domainName ? `https://${apiCustomDomain?.domainName}` : '',
        PIECE_BUFFER_QUEUE_URL: pieceBufferQueue.queueUrl,
        PIECE_BUFFER_QUEUE_REGION: stack.region
      },
      permissions: [
        pieceBufferQueue
      ],
      bind: [
        privateKey
      ]
    },
    cdk: {
      eventSource: {
        batchSize: stack.stage === 'production' ?
          25 // Production max out batch write to dynamo
          : 10, // Integration tests
        maxBatchingWindow: stack.stage === 'production' ?
          Duration.minutes(1) // Production max out batch write to dynamo
          : Duration.seconds(5), // Integration tests
        // allow reporting partial failures
        reportBatchItemFailures: true,
      },
    },
  })

  /**
   * Handle queued pieces received from multiple producers by batching them into buffers
   */
  pieceBufferQueue.addConsumer(stack, {
    function: {
      handler: 'packages/functions/src/processor/piece-buffering.workflow',
      environment: {
        BUFFER_QUEUE_URL: bufferQueue.queueUrl,
        BUFFER_QUEUE_REGION: stack.region,
        BUFFER_STORE_BUCKET_NAME: bufferStoreBucket.bucketName,
        BUFFER_STORE_REGION: stack.region,
      },
      permissions: [
        bufferQueue,
        bufferStoreBucket
      ],
    },
    cdk: {
      eventSource: {
        batchSize: stack.stage === 'production' ?
          10_000 // Production max out batch size
          : 10, // Integration tests
        maxBatchingWindow: stack.stage === 'production' ?
          Duration.minutes(5) // Production max out batch write to dynamo
          : Duration.seconds(5), // Integration tests
        // allow reporting partial failures
        reportBatchItemFailures: true,
      },
    },
  })

  /**
   * Handle queued piece buffers by concatening buffers until an aggregate can be created
   */
  bufferQueue.addConsumer(stack, {
    function: {
      handler: 'packages/functions/src/processor/buffer-reducing.workflow',
      environment: {
        BUFFER_QUEUE_URL: bufferQueue.queueUrl,
        BUFFER_QUEUE_REGION: stack.region,
        BUFFER_STORE_BUCKET_NAME: bufferStoreBucket.bucketName,
        BUFFER_STORE_REGION: stack.region,
        AGGREGATE_QUEUE_URL: aggregateQueue.queueUrl,
        AGGREGATE_QUEUE_REGION: stack.region,
        MAX_AGGREGATE_SIZE,
        MIN_AGGREGATE_SIZE,
      },
      permissions: [
        bufferQueue,
        bufferStoreBucket,
        aggregateQueue
      ]
    },
    cdk: {
      eventSource: {
        // as soon as we have 2, we can act fast and reduce to see if enough bytes
        batchSize: 2,
        // allow reporting partial failures
        reportBatchItemFailures: true,
      },
    },
  })

  /**
   * Handle queued aggregates to be sent to dealer
   */
  aggregateQueue.addConsumer(stack, {
    function: {
      handler: 'packages/functions/src/processor/dealer-queue.workflow',
      environment: {
        BUFFER_STORE_BUCKET_NAME: bufferStoreBucket.bucketName,
        BUFFER_STORE_REGION: stack.region,
        DID,
        DEALER_DID,
        DEALER_URL
      },
      permissions: [
        bufferStoreBucket,
      ],
      bind: [
        aggregateStoreTable,
        privateKey,
      ],
    },
    cdk: {
      eventSource: {
        // as soon as we have one, we should queue it to the dealer
        batchSize: 1,
        reportBatchItemFailures: true,
      },
    },
  })

  // testing grouping
  const groupingQueueName = getResourceName('grouping-queue', stack.stage)
  const groupingQueue = new Queue(stack, groupingQueueName, {
    cdk: {
      queue: {
        // Guarantee exactly-once processing
        // (Note: maximum 10 batch)
        fifo: true,
        // During the deduplication interval (5 minutes), Amazon SQS treats
        // messages that are sent with identical body content
        contentBasedDeduplication: true,
        queueName: `${groupingQueueName}.fifo`,
        receiveMessageWaitTime: Duration.seconds(20),
      }
    }
  })

  /**
   * Handle queued aggregates to be sent to dealer
   */
  groupingQueue.addConsumer(stack, {
    function: {
      handler: 'packages/functions/src/processor/grouping.workflow',
      environment: {},
    },
    cdk: {
      eventSource: {
        // as soon as we have one, we should queue it to the dealer
        batchSize: 3,
        reportBatchItemFailures: true,
        // maxConcurrency: 1
        // maxBatchingWindow: Duration.seconds(30)
      },
    },
  })

  stack.addOutputs({
    groupingQueueUrl: groupingQueue.queueUrl,
    groupingQueueRegion: stack.region,
    groupingQueueName: groupingQueueName,
  })

  return {
    pieceAddQueue,
    pieceBufferQueue,
    bufferQueue,
    aggregateQueue,
    privateKey
  }
}
