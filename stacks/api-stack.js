import { Api, Config, use } from 'sst/constructs'

import { ProcessorStack } from './processor-stack.js'
import {
  getApiPackageJson,
  getGitInfo,
  getCustomDomain,
  setupSentry
} from './config.js'

/**
 * @param {import('sst/constructs').StackContext} properties
 */
export function ApiStack({ app, stack }) {
  // Setup app monitoring with Sentry
  setupSentry(app, stack)

  const {
    pieceQueue,
    privateKey
  } = use(ProcessorStack)

  // Setup API
  const customDomain = getCustomDomain(stack.stage, process.env.HOSTED_ZONE)
  const pkg = getApiPackageJson()
  const git = getGitInfo()
  const ucanLogBasicAuth = new Config.Secret(stack, 'UCAN_LOG_BASIC_AUTH')

  const api = new Api(stack, 'api', {
    customDomain,
    defaults: {
      function: {
        environment: {
          NAME: pkg.name,
          VERSION: pkg.version,
          COMMIT: git.commit,
          STAGE: stack.stage,
          DID: process.env.DID ?? '',
          BROKER_DID: process.env.BROKER_DID ?? '',
          BROKER_URL: process.env.BROKER_URL ?? '',
          UCAN_LOG_URL: process.env.UCAN_LOG_URL ?? '',
          PIECE_QUEUE_URL: pieceQueue.queueUrl,
          PIECE_QUEUE_REGION: stack.region
        },
        bind: [
          privateKey,
          ucanLogBasicAuth
        ]
      }
    },
    routes: {
      'GET /':        'packages/functions/src/api/get.home',
      'GET /error':   'packages/functions/src/api/get.error',
      'GET /version': 'packages/functions/src/api/get.version',
      'POST /':       'packages/functions/src/api/ucan-invocation-router.handler',
    },
  })

  stack.addOutputs({
    ApiEndpoint: api.url,
    CustomDomain:  customDomain ? `https://${customDomain.domainName}` : 'Set HOSTED_ZONE in env to deploy to a custom domain'
  })
}