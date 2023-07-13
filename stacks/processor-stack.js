import {
  Cron,
  Function,
  Queue,
  use
} from 'sst/constructs'

import { DbStack } from './db-stack.js'
import {
  setupSentry,
  getEnv
} from './config.js'

/**
 * @param {import('sst/constructs').StackContext} properties
 */
export function ProcessorStack({ stack, app }) {
  const { CONTENT_RESOLVER_URL_R2 } = getEnv()
  // Setup app monitoring with Sentry
  setupSentry(app, stack)

  const { db } = use(DbStack)

  // `piece-maker` workflow:
  // - CRON resource calling `piece-maker` lambda
  // - lambda for consuming `piece-maker` DB view and queue its processing
  // - QUEUE triggers lambda functions to derive piece CIDs from received content
  const pieceMakerProducerHandler = new Function(
    stack,
    'piece-maker-producer-handler',
    {
      handler: 'packages/functions/src/workflow/piece-maker.producer',
      bind: [db],
      environment: {
        CONTENT_RESOLVER_URL_R2
      }
    }
  )
  const pieceMakerItems = new Queue(
    stack,
    'piece-maker-producer-queue',
    {
      consumer: {
        function: pieceMakerProducerHandler,
        cdk: {
          eventSource: {
            batchSize: 1,
          },
        }
      }
      // TODO: DLQ
    }
  )

  new Cron(
    stack,
    'piece-maker-consumer',
    {
      schedule: 'rate(15 minutes)',
      job: {
        function: {
          handler: 'packages/functions/src/workflow/piece-maker.consumer',
          environment: {
            QUEUE_URL: pieceMakerItems.queueUrl,
            QUEUE_REGION: stack.region
          },
          bind: [db],
        },
      }
    }
  )
}
