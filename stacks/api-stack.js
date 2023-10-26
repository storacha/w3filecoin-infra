import { Api, Config, use } from 'sst/constructs'

import { DataStack } from './data-stack.js'
import { ProcessorStack } from './processor-stack.js'
import {
  getApiPackageJson,
  getGitInfo,
  getCustomDomain,
  getAggregatorEnv,
  getDealTrackerEnv,
  setupSentry
} from './config.js'

/**
 * @param {import('sst/constructs').StackContext} properties
 */
export function ApiStack({ app, stack }) {
  const {
    DID,
    DEALER_DID,
    DEALER_URL,
    UCAN_LOG_URL,
    HOSTED_ZONE
  } = getAggregatorEnv(stack)
  const {
    DEAL_TRACKER_API_HOSTED_ZONE,
    DEAL_TRACKER_DID
  } = getDealTrackerEnv()

  // Setup app monitoring with Sentry
  setupSentry(app, stack)

  const {
    pieceStoreTable,
    dealTrackerDealStoreTable,
    privateKey,
    dealTrackerPrivateKey,
    dealerPrivateKey,
    dealerAggregateStoreTable,
    dealerOfferStoreBucket
  } = use(DataStack)
  const { pieceAddQueue } = use(ProcessorStack)
  const pkg = getApiPackageJson()
  const git = getGitInfo()
  const ucanLogBasicAuth = new Config.Secret(stack, 'UCAN_LOG_BASIC_AUTH')

  // Setup `aggregator-api`
  const aggregatorApiCustomDomain = getCustomDomain(stack.stage, HOSTED_ZONE)
  const api = new Api(stack, 'aggregator-api', {
    customDomain: aggregatorApiCustomDomain,
    defaults: {
      function: {
        environment: {
          NAME: pkg.name,
          VERSION: pkg.version,
          COMMIT: git.commit,
          STAGE: stack.stage,
          DID,
          DEALER_DID,
          DEALER_URL,
          UCAN_LOG_URL,
          PIECE_ADD_QUEUE_URL: pieceAddQueue.queueUrl,
          PIECE_ADD_QUEUE_REGION: stack.region
        },
        bind: [
          privateKey,
          ucanLogBasicAuth,
          pieceStoreTable,
          pieceAddQueue
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
  const dealerApiCustomDomain = getCustomDomain(stack.stage, DEAL_TRACKER_API_HOSTED_ZONE)
  const dealerApi = new Api(stack, 'dealer-api', {
    customDomain: dealerApiCustomDomain,
    defaults: {
      function: {
        environment: {
          NAME: pkg.name,
          VERSION: pkg.version,
          COMMIT: git.commit,
          STAGE: stack.stage,
          DID: DEALER_DID,
          UCAN_LOG_URL,
          OFFER_STORE_BUCKET_NAME: dealerOfferStoreBucket.bucketName,
          OFFER_STORE_BUCKET_REGION: stack.region,
        },
        bind: [
          dealerPrivateKey,
          dealerAggregateStoreTable,
          ucanLogBasicAuth,
        ]
      }
    },
    routes: {
      'GET /':        'packages/functions/src/dealer-api/get.home',
      'GET /error':   'packages/functions/src/dealer-api/get.error',
      'GET /version': 'packages/functions/src/dealer-api/get.version',
      'POST /':       'packages/functions/src/dealer-api/ucan-invocation-router.handler',
    },
  })

  // Setup `deal-tracker-api`
  const dealTrackerApiCustomDomain = getCustomDomain(stack.stage, DEAL_TRACKER_API_HOSTED_ZONE)
  const dealTrackerApi = new Api(stack, 'deal-tracker-api', {
    customDomain: dealTrackerApiCustomDomain,
    defaults: {
      function: {
        environment: {
          NAME: pkg.name,
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

  stack.addOutputs({
    AggregateApiEndpoint: api.url,
    AggregateApiCustomDomain: aggregatorApiCustomDomain ? `https://${aggregatorApiCustomDomain.domainName}` : 'Set HOSTED_ZONE in env to deploy to a custom domain',
    DealTrackerApiEndpoint: dealTrackerApi.url,
    DealTrackerApiCustomDomain: dealTrackerApiCustomDomain ? `https://${dealTrackerApiCustomDomain.domainName}` : 'Set HOSTED_ZONE in env to deploy to a custom domain',
    DealerApiEndpoint: dealerApi.url,
    DealerApiCustomDomain: dealerApiCustomDomain ? `https://${dealerApiCustomDomain.domainName}` : 'Set HOSTED_ZONE in env to deploy to a custom domain',
  })
}