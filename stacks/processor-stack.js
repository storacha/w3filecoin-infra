import { Queue, Config, use } from 'sst/constructs'
import { Duration } from 'aws-cdk-lib'

import { DataStack } from './data-stack.js'
import {
  // setupSentry,
  getEnv,
  getResourceName
} from './config.js'

/**
 * @param {import('sst/constructs').StackContext} properties
 */
export function ProcessorStack({ stack, app }) {
  const { DID, BROKER_DID, BROKER_URL } = getEnv()
  // Setup app monitoring with Sentry
  // setupSentry(app, stack)
  // TODO: enable

  const privateKey = new Config.Secret(stack, 'PRIVATE_KEY')

  const {
    bufferStoreBucket,
    aggregateStoreTable
  } = use(DataStack)

  // TODO: Events from piece table to piece-queue

  /**
   * 1st processor queue - piece buffering workflow
   */
  const pieceQueueName = getResourceName('piece-queue', stack.stage)
  const pieceQueue = new Queue(stack, pieceQueueName, {
    cdk: {
      queue: {
        // During the deduplication interval (5 minutes), Amazon SQS treats
        // messages that are sent with identical body content
        contentBasedDeduplication: true,
        queueName: `${pieceQueueName}.fifo`
      }
    }
  })

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
   * Handle queued pieces received from multiple producers by batching them into buffers
   */
  pieceQueue.addConsumer(stack, {
    function: {
      handler: 'packages/functions/src/processor/piece-buffering.workflow',
      bind: [
        bufferStoreBucket
      ],
      environment: {
        BUFFER_QUEUE_URL: bufferQueue.queueUrl,
        BUFFER_QUEUE_REGION: stack.region
      }
    },
    cdk: {
      eventSource: {
        batchSize: 10_000,
        maxBatchingWindow: Duration.minutes(5)
      },
    },
  })

  /**
   * Handle queued piece buffers by concatening buffers until an aggregate can be created
   */
  bufferQueue.addConsumer(stack, {
    function: {
      handler: 'packages/functions/src/processor/buffer-reducing.workflow',
      bind: [
        bufferStoreBucket
      ],
      environment: {
        BUFFER_QUEUE_URL: bufferQueue.queueUrl,
        BUFFER_QUEUE_REGION: stack.region,
        AGGREGATE_QUEUE_URL: aggregateQueue.queueUrl,
        AGGREGATE_QUEUE_REGION: stack.region
      }
    },
    cdk: {
      eventSource: {
        // as soon as we have 2, we can act fast and recuce to see if enough bytes
        batchSize: 2,
        maxBatchingWindow: Duration.minutes(5)
      },
    },
  })

  /**
   * Handle queued aggregates to be added to broker
   */
  aggregateQueue.addConsumer(stack, {
    function: {
      handler: 'packages/functions/src/processor/aggregate-add.workflow',
      bind: [
        bufferStoreBucket,
        aggregateStoreTable
      ],
      environment: {
        DID,
        BROKER_DID,
        BROKER_URL,
      }
    },
    cdk: {
      eventSource: {
        // as soon as we have one, we should add it to broker for deal
        batchSize: 1,
        maxBatchingWindow: Duration.minutes(5)
      },
    },
  })

  return {
    pieceQueue,
    bufferQueue,
    aggregateQueue,
    privateKey
  }
}
