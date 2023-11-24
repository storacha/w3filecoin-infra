import { Api, Config, Cron, use } from 'sst/constructs'

import { DataStack } from './data-stack.js'
import { AggregatorStack } from './aggregator-stack.js'
import {
  getApiPackageJson,
  getGitInfo,
  getCustomDomain,
  getEnv,
  getAggregatorEnv,
  getDealerEnv,
  getDealTrackerEnv,
  setupSentry,
  getResourceName
} from './config.js'

/**
 * @param {import('sst/constructs').StackContext} properties
 */
export function ApiStack({ app, stack }) {
  const {
    AGGREGATOR_HOSTED_ZONE,
    AGGREGATOR_DID,
    UCAN_LOG_URL,
  } = getAggregatorEnv(stack)
  const {
    DEAL_TRACKER_API_HOSTED_ZONE,
    DEAL_TRACKER_DID,
    DEAL_TRACKER_PROOF
  } = getDealTrackerEnv()
  const {
    DEAL_API_HOSTED_ZONE,
    DEALER_DID,
  } = getDealerEnv()
  const {
    MIN_PIECE_CRITICAL_THRESHOLD_MS,
    MIN_PIECE_WARN_THRESHOLD_MS,
    AGGREGATE_MONITOR_THRESHOLD_MS,
    MONITORING_NOTIFICATIONS_ENDPOINT
  } = getEnv()

  // Setup app monitoring with Sentry
  setupSentry(app, stack)

  const {
    // Private keys
    aggregatorPrivateKey,
    dealerPrivateKey,
    dealTrackerPrivateKey,
    // Aggregator stores
    aggregatorBufferStoreBucket,
    aggregatorPieceStoreTable,
    aggregatorAggregateStoreTable,
    aggregatorInclusionStoreTable,
    aggregatorInclusionProofStoreBucket,
    // Deal tracker stores
    dealTrackerDealStoreTable,
    // Dealer stores
    dealerAggregateStoreTable,
    dealerOfferStoreBucket
  } = use(DataStack)
  const {
    pieceQueue,
    bufferQueue,
    aggregateOfferQueue,
    pieceAcceptQueue
  } = use(AggregatorStack)
  const pkg = getApiPackageJson()
  const git = getGitInfo()
  const ucanLogBasicAuth = new Config.Secret(stack, 'UCAN_LOG_BASIC_AUTH')
  const dealTrackerApiCustomDomain = getCustomDomain(stack.stage, DEAL_TRACKER_API_HOSTED_ZONE)

  // Setup `aggregator-api`
  const aggregatorApiCustomDomain = getCustomDomain(stack.stage, AGGREGATOR_HOSTED_ZONE)
  const api = new Api(stack, 'aggregator-api', {
    customDomain: aggregatorApiCustomDomain,
    defaults: {
      function: {
        environment: {
          NAME: 'aggregator-api',
          VERSION: pkg.version,
          COMMIT: git.commit,
          STAGE: stack.stage,
          DID: AGGREGATOR_DID,
          DEALER_DID,
          UCAN_LOG_URL,
          PIECE_QUEUE_URL: pieceQueue.queueUrl,
          BUFFER_QUEUE_URL: bufferQueue.queueUrl,
          AGGREGATE_OFFER_QUEUE_URL: aggregateOfferQueue.queueUrl,
          PIECE_ACCEPT_QUEUE_URL: pieceAcceptQueue.queueUrl,
          BUFFER_STORE_BUCKET_NAME: aggregatorBufferStoreBucket.bucketName,
          INCLUSION_PROOF_STORE_BUCKET_NAME: aggregatorInclusionProofStoreBucket.bucketName,
        },
        bind: [
          aggregatorPrivateKey,
          ucanLogBasicAuth,
          aggregatorAggregateStoreTable,
          aggregatorInclusionStoreTable,
          aggregatorPieceStoreTable,
        ],
        permissions: [
          aggregatorBufferStoreBucket,
          aggregatorInclusionProofStoreBucket,
          pieceQueue,
          bufferQueue,
          aggregateOfferQueue,
          pieceAcceptQueue
        ]
      }
    },
    routes: {
      'GET /':        'packages/functions/src/aggregator-api/get.home',
      'GET /error':   'packages/functions/src/aggregator-api/get.error',
      'GET /version': 'packages/functions/src/aggregator-api/get.version',
      'POST /':       'packages/functions/src/aggregator-api/ucan-invocation-router.handler',
    },
  })

  // Setup `dealer-api`
  const dealerApiCustomDomain = getCustomDomain(stack.stage, DEAL_API_HOSTED_ZONE)
  const dealerApi = new Api(stack, 'dealer-api', {
    customDomain: dealerApiCustomDomain,
    defaults: {
      function: {
        environment: {
          NAME: 'dealer-api',
          VERSION: pkg.version,
          COMMIT: git.commit,
          STAGE: stack.stage,
          DID: DEALER_DID,
          SERVICE_DID: DEAL_TRACKER_DID,
          SERVICE_URL: dealTrackerApiCustomDomain?.domainName ? `https://${dealTrackerApiCustomDomain?.domainName}` : '',
          PROOF: DEAL_TRACKER_PROOF,
          UCAN_LOG_URL,
          OFFER_STORE_BUCKET_NAME: dealerOfferStoreBucket.bucketName,
          OFFER_STORE_BUCKET_REGION: stack.region,
        },
        bind: [
          dealerPrivateKey,
          dealerAggregateStoreTable,
          ucanLogBasicAuth,
        ],
        permissions: [
          dealerOfferStoreBucket
        ]
      }
    },
    routes: {
      'GET /':        'packages/functions/src/dealer-api/get.home',
      'GET /error':   'packages/functions/src/dealer-api/get.error',
      'GET /version': 'packages/functions/src/dealer-api/get.version',
      'GET /cron':    'packages/functions/src/dealer/handle-cron-tick.main',
      'POST /':       'packages/functions/src/dealer-api/ucan-invocation-router.handler',
    },
  })

  // Setup `deal-tracker-api`
  const dealTrackerApi = new Api(stack, 'deal-tracker-api', {
    customDomain: dealTrackerApiCustomDomain,
    defaults: {
      function: {
        environment: {
          NAME: 'deal-tracker-api',
          VERSION: pkg.version,
          COMMIT: git.commit,
          STAGE: stack.stage,
          DID: DEAL_TRACKER_DID,
          UCAN_LOG_URL
        },
        bind: [
          dealTrackerPrivateKey,
          dealTrackerDealStoreTable,
          ucanLogBasicAuth,
        ]
      }
    },
    routes: {
      'GET /':        'packages/functions/src/deal-tracker-api/get.home',
      'GET /error':   'packages/functions/src/deal-tracker-api/get.error',
      'GET /version': 'packages/functions/src/deal-tracker-api/get.version',
      'POST /':       'packages/functions/src/deal-tracker-api/ucan-invocation-router.handler',
    },
  })

  // Setup `monitoring`
  // only needed for production
  if (stack.stage === 'prod') {
    const dealMonitorAlertCronName = getResourceName('deal-monitor-alert-cron', stack.stage)
    new Cron(stack, dealMonitorAlertCronName, {
      schedule: 'rate(30 minutes)',
      job: {
        function: {
          timeout: '5 minutes',
          handler: 'packages/functions/src/monitor/handle-deal-monitor-alert-cron-tick.main',
          environment: {
            MIN_PIECE_CRITICAL_THRESHOLD_MS,
            MIN_PIECE_WARN_THRESHOLD_MS,
            AGGREGATE_MONITOR_THRESHOLD_MS,
            MONITORING_NOTIFICATIONS_ENDPOINT
          },
          bind: [
            dealerAggregateStoreTable,
            aggregatorAggregateStoreTable,
          ],
        }
      }
    })
  }

  stack.addOutputs({
    AggregatorApiEndpoint: api.url,
    AggregatorApiCustomDomain: aggregatorApiCustomDomain ? `https://${aggregatorApiCustomDomain.domainName}` : 'Set AGGREGATOR_HOSTED_ZONE in env to deploy to a custom domain',
    DealTrackerApiEndpoint: dealTrackerApi.url,
    DealTrackerApiCustomDomain: dealTrackerApiCustomDomain ? `https://${dealTrackerApiCustomDomain.domainName}` : 'Set DEAL_TRACKER_API_HOSTED_ZONE in env to deploy to a custom domain',
    DealerApiEndpoint: dealerApi.url,
    DealerApiCustomDomain: dealerApiCustomDomain ? `https://${dealerApiCustomDomain.domainName}` : 'Set DEALER_API_HOSTED_ZONE in env to deploy to a custom domain',
  })

  return {
    aggregateApiEndpoint: api.url,
    dealTrackerApiEndpoint: dealTrackerApi.url,
    dealerApiEndpoint: dealerApi.url,
  }
}