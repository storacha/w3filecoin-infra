import { Api } from '@serverless-stack/resources'

import {
  getApiPackageJson,
  getGitInfo,
  getCustomDomain,
  setupSentry
} from './config.js'

/**
 * @param {import('@serverless-stack/resources').StackContext} properties
 */
export function ApiStack({ app, stack }) {
  stack.setDefaultFunctionProps({
    srcPath: 'api'
  })

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
          COMMIT: git.commmit,
          STAGE: stack.stage,
        },
        timeout: 15 * 60,
      },
    },
    routes: {
      'GET /':        'functions/get.home',
      'GET /error':   'functions/get.error',
      'GET /version': 'functions/get.version'
    },
  })

  stack.addOutputs({
    ApiEndpoint: api.url,
    CustomDomain:  customDomain ? `https://${customDomain.domainName}` : 'Set HOSTED_ZONE in env to deploy to a custom domain'
  })
}
