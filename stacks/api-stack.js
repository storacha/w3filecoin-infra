import { Api } from 'sst/constructs'

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

  // Setup API
  const customDomain = getCustomDomain(stack.stage, process.env.HOSTED_ZONE)
  const pkg = getApiPackageJson()
  const git = getGitInfo()

  const api = new Api(stack, 'api', {
    customDomain,
    defaults: {
      function: {
        environment: {
          NAME: pkg.name,
          VERSION: pkg.version,
          COMMIT: git.commit,
          STAGE: stack.stage,
        }
      }
    },
    routes: {
      'GET /':        'packages/functions/src/api/get.home',
      'GET /error':   'packages/functions/src/api/get.error',
      'GET /version': 'packages/functions/src/api/get.version'
    },
  })

  stack.addOutputs({
    ApiEndpoint: api.url,
    CustomDomain:  customDomain ? `https://${customDomain.domainName}` : 'Set HOSTED_ZONE in env to deploy to a custom domain'
  })
}