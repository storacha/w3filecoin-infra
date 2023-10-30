import { Cron, use } from 'sst/constructs'
import { StartingPosition } from 'aws-cdk-lib/aws-lambda'

import { Status } from '../packages/core/src/store/dealer-aggregate-store.js'
import { ApiStack } from './api-stack.js'
import { DataStack } from './data-stack.js'
import {
  setupSentry,
  getDealerEnv,
  getDealTrackerEnv,
  getResourceName
} from './config.js'

/**
 * @param {import('sst/constructs').StackContext} properties
 */
export function DealerStack({ stack, app }) {
  const {
    DEAL_TRACKER_DID,
    DEAL_TRACKER_PROOF
  } = getDealTrackerEnv()
  const {
    DEALER_DID,
    DEALER_PROOF
  } = getDealerEnv()

  // Setup app monitoring with Sentry
  setupSentry(app, stack)

  // Get dependent stacks references
  const {
    dealerApiEndpoint,
    dealTrackerApiEndpoint,
  } = use(ApiStack)
  const {
    dealerAggregateStoreTable,
    dealerOfferStoreBucket,
    dealerPrivateKey,
  } = use(DataStack)

  /**
   * DynamoDB Stream consumer that on INSERT event will update offer store entry key with date,
   * so that broker can retrieve it by date prefix.
   */
  dealerAggregateStoreTable.addConsumers(stack, {
    handleAggregateInsert: {
      function: {
        handler: 'packages/functions/src/dealer/handle-aggregate-insert.main',
        environment: {
          OFFER_STORE_BUCKET_NAME: dealerOfferStoreBucket.bucketName,
          OFFER_STORE_BUCKET_REGION: stack.region,
        },
        permissions: [
          dealerOfferStoreBucket
        ]
      },
      cdk: {
        // https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_lambda_event_sources.DynamoEventSourceProps.html#filters
        eventSource: {
          batchSize: 1,
          // Start reading at the last untrimmed record in the shard in the system.
          startingPosition: StartingPosition.TRIM_HORIZON,
        },
      },
      filters: [
        {
          eventName: ['INSERT'],
          dynamodb: {
            NewImage: {
              stat: {
                N: [`${Status.OFFERED}`]
              }
            }
          }
        }
      ]
    }
  })

  /**
   * DynamoDB Stream consumer that on UPDATE event will issue `aggregate/accept` receipt.
   */
  dealerAggregateStoreTable.addConsumers(stack, {
    handleAggregatUpdatedStatus: {
      function: {
        handler: 'packages/functions/src/dealer/handle-aggregate-updated-status.main',
        environment: {
          DID: DEALER_DID,
          SERVICE_DID: DEALER_DID,
          SERVICE_URL: dealerApiEndpoint,
          PROOF: DEALER_PROOF,
        },
        bind: [
          dealerPrivateKey
        ]
      },
      cdk: {
        // https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_lambda_event_sources.DynamoEventSourceProps.html#filters
        eventSource: {
          batchSize: 1,
          // Start reading at the last untrimmed record in the shard in the system.
          startingPosition: StartingPosition.TRIM_HORIZON,
        },
      },
      filters: [
        // Trigger when status value changed from OFFERED
        {
          dynamodb: {
            NewImage: {
              stat: {
                N: [`${Status.ACCEPTED}`, `${Status.INVALID}`]
              }
            }
          }
        }
      ]
    }
  })

  /**
   * CRON to track deals pending resolution.
   * On cron tick event, get aggregates without deals, and verify if there are updates on them.
   * If there are deals for pending aggregates, their state can be updated.
   */
  const dealTrackCronName = getResourceName('deal-track-cron', stack.stage)
  new Cron(stack, dealTrackCronName, {
    schedule: 'rate(5 minutes)',
    job: {
      function: {
        handler: 'packages/functions/src/dealer/handle-cron-tick.main',
        environment: {
          DID: DEALER_DID,
          SERVICE_DID: DEAL_TRACKER_DID,
          SERVICE_URL: dealTrackerApiEndpoint,
          PROOF: DEAL_TRACKER_PROOF,
        },
        bind: [
          dealerAggregateStoreTable,
          dealerPrivateKey
        ],
      }
    }
  })
}
