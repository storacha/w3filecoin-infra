import {
  Cron,
  Config,
  Function,
  Queue,
  use
} from 'sst/constructs'
import { Duration } from 'aws-cdk-lib'

import { DbStack } from './db-stack.js'
import {
  setupSentry,
  getEnv,
  getResourceName
} from './config.js'

/**
 * @param {import('sst/constructs').StackContext} properties
 */
export function ProcessorStack({ stack, app }) {
  // Setup app monitoring with Sentry
  setupSentry(app, stack)

  const {
    CONTENT_RESOLVER_URL_R2,
    DID,
    AGGREGATION_SERVICE_DID,
    AGGREGATION_SERVICE_URL
  } = getEnv()
  const privateKey = new Config.Secret(stack, 'PRIVATE_KEY')

  const { db } = use(DbStack)

  // `piece-maker` workflow:
  // - CRON resource calling `piece-maker` lambda
  // - lambda for consuming `piece-maker` DB view and queue its processing
  // - QUEUE triggers lambda functions to derive piece CIDs from received content
  const pieceMakerHandler = new Function(
    stack,
    'piece-maker-handler',
    {
      handler: 'packages/functions/src/workflow/piece-maker.build',
      bind: [db],
      environment: {
        CONTENT_RESOLVER_URL_R2
      },
      timeout: 15 * 60,
    }
  )

  const pieceMakerQueueName = getResourceName('piece-maker-queue', stack.stage)
  const pieceMakerItems = new Queue(
    stack,
    pieceMakerQueueName,
    {
      cdk: {
        queue: {
          // Needs to be set as less or equal than consumer function
          visibilityTimeout: Duration.seconds(15 * 60),
          // During the deduplication interval (5 minutes), Amazon SQS treats
          // messages that are sent with identical body content
          contentBasedDeduplication: true,
          queueName: `${pieceMakerQueueName}.fifo`
        }
      },
      consumer: {
        function: pieceMakerHandler,
        cdk: {
          eventSource: {
            batchSize: 1,
          },
        },
      },
      // TODO: DLQ
    },
  )

  new Cron(
    stack,
    'piece-maker-consumer',
    {
      schedule: 'rate(15 minutes)',
      job: {
        function: {
          handler: 'packages/functions/src/workflow/piece-maker.consume',
          environment: {
            QUEUE_URL: pieceMakerItems.queueUrl,
            QUEUE_REGION: stack.region,
          },
          bind: [db],
          timeout: 15 * 60,
        },
      }
    }
  )

  // `aggregator`
  new Cron(
    stack,
    'aggregator-consumer',
    {
      schedule: 'rate(10 minutes)',
      job: {
        function: {
          handler: 'packages/functions/src/workflow/aggregator.run',
          environment: {},
          bind: [db],
          timeout: 15 * 60,
        },
      }
    }
  )

  // `submission` workflow
  // - CRON resource calling `submission` consume lambda
  // - lambda for consuming `aggregate_queue` DB view and queue its processing
  // - QUEUE triggers lambda functions to submit aggregate offers
  const submitOfferHandler = new Function(
    stack,
    'submit-offer-handler',
    {
      handler: 'packages/functions/src/workflow/submission.build',
      bind: [
        db,
        privateKey
      ],
      environment: {
        DID,
        AGGREGATION_SERVICE_DID,
        AGGREGATION_SERVICE_URL,
      },
      timeout: 15 * 60,
    }
  )
  const submitOfferQueueName = getResourceName('submit-offer-queue', stack.stage)
  const submitOfferItems = new Queue(
    stack,
    submitOfferQueueName,
    {
      cdk: {
        queue: {
          // Needs to be set as less or equal than consumer function
          visibilityTimeout: Duration.seconds(15 * 60),
          // During the deduplication interval (5 minutes), Amazon SQS treats
          // messages that are sent with identical body content
          contentBasedDeduplication: true,
          queueName: `${submitOfferQueueName}.fifo`
        }
      },
      consumer: {
        function: submitOfferHandler,
        cdk: {
          eventSource: {
            batchSize: 1,
          },
        },
      },
      // TODO: DLQ
    },
  )

  new Cron(
    stack,
    'submission-consumer',
    {
      schedule: 'rate(10 minutes)',
      job: {
        function: {
          handler: 'packages/functions/src/workflow/submission.consume',
          environment: {
            QUEUE_URL: submitOfferItems.queueUrl,
            QUEUE_REGION: stack.region,
          },
          bind: [db],
          timeout: 15 * 60,
        },
      }
    }
  )
}
