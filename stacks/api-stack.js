import { Api } from '@serverless-stack/resources'

import { getCustomDomain } from './config.js'

/**
 * @param {import('@serverless-stack/resources').StackContext} properties
 */
export function ApiStack({ stack }) {
  stack.setDefaultFunctionProps({
    srcPath: 'api'
  })

  // Setup API
  const customDomain = getCustomDomain(stack.stage, process.env.HOSTED_ZONE)

  const api = new Api(stack, 'api', {
    customDomain,
    routes: {
      'GET /': 'functions/lambda.handler',
    },
  })

  stack.addOutputs({
    ApiEndpoint: api.url,
  })
}
