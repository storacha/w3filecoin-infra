import { Api } from '@serverless-stack/resources'

/**
 * @param {import('@serverless-stack/resources').StackContext} properties
 */
export function ApiStack({ stack }) {
  stack.setDefaultFunctionProps({
    srcPath: 'api'
  })

  const api = new Api(stack, 'api', {
    routes: {
      'GET /': 'functions/lambda.handler',
    },
  })

  stack.addOutputs({
    ApiEndpoint: api.url,
  })
}
