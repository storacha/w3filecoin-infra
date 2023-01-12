import { ApiStack } from './api-stack.js'

/**
 * @param {import('@serverless-stack/resources').App} app
 */
export default function (app) {
  app.setDefaultFunctionProps({
    runtime: 'nodejs16.x',
    environment: {
      NODE_OPTIONS: "--enable-source-maps",
    },
    bundle: {
      format: 'esm',
    },
  })
  app.stack(ApiStack)
}
