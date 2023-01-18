import {
  EventBus,
  Function,
  Queue,
  use
} from '@serverless-stack/resources'
import * as events from 'aws-cdk-lib/aws-events'
import { Duration } from 'aws-cdk-lib'

import { DataStack } from './data-stack.js'
import { setupSentry } from './config.js'

const CARPARK_EVENT_BRIDGE_SOURCE_EVENT = 'carpark_bucket'

/**
 * @param {import('@serverless-stack/resources').StackContext} properties
 */
export function ConsumerStack({ stack, app }) {
  stack.setDefaultFunctionProps({
    srcPath: 'consumer'
  })

  // Setup app monitoring with Sentry
  setupSentry(app, stack)
  const { W3INFRA_EVENT_BRIDGE_ARN } = getEnv()

  // Get references to constructs created in other stacks
  const { carTable } = use(DataStack)

  // CAR metadata writer lambda
  const carMetadataWriterHandler = new Function(
    stack,
    'car-metadata-writer-handler',
    {
      environment: {
        CAR_TABLE_NAME: carTable.tableName,
      },
      permissions: [carTable],
      handler: 'functions/car-metadata-writer.handler',
      timeout: 15 * 60,
    }
  )

  // Queue
  const carWriteToAggregateQueue = new Queue(stack, 'car-to-aggregate', {
    consumer: {
      function: carMetadataWriterHandler,
      cdk: {
        eventSource: {
          // Maximum batch size must be between 1 and 10 inclusive when batching window is not specified.
          // TODO: need window
          batchSize: 50,
          maxBatchingWindow: Duration.minutes(5),
        },
      }
    },
    cdk: {
      queue: {
        // Needs to be set as less or equal than consumer function
        visibilityTimeout: Duration.seconds(15 * 60),
      },
    },
  })

  // Event bus bind
  const eventBus = new EventBus(stack, 'event-bus', {
    // @ts-expect-error types not match...
    eventBridgeEventBus: events.EventBus.fromEventBusArn(stack, 'imported-event-bus', W3INFRA_EVENT_BRIDGE_ARN)
  })

  eventBus.addRules(stack, {
    newCar: {
      pattern: {
        // TODO: this needs to be from replicator!!
        source: [CARPARK_EVENT_BRIDGE_SOURCE_EVENT],
      },
      targets: {
        writeAggregateQueueTarget: {
          type: 'queue',
          queue: carWriteToAggregateQueue,
          cdk: {
            // custom target message
            target: {
              message: events.RuleTargetInput.fromObject({
                bucketRegion: events.EventField.fromPath('$.detail.region'),
                bucketName: events.EventField.fromPath('$.detail.bucketName'),
                key: events.EventField.fromPath('$.detail.key')
              }),
            }
          }
        },
      }
    }
  })
}

/**
 * Get Env validating it is set.
 */
function getEnv() {
  return {
    W3INFRA_EVENT_BRIDGE_ARN: mustGetEnv('W3INFRA_EVENT_BRIDGE_ARN'),
  }
}

/**
 * 
 * @param {string} name 
 * @returns {string}
 */
function mustGetEnv (name) {
  if (!process.env[name]) {
    throw new Error(`Missing env var: ${name}`)
  }

  // @ts-expect-error there will always be a string there, but typescript does not believe
  return process.env[name]
}
