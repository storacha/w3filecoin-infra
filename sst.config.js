import { Tags } from 'aws-cdk-lib';

import { ApiStack } from './stacks/api-stack.js'
import { ProcessorStack } from './stacks/processor-stack.js'

export default {
  config() {
    return {
      name: 'w3filecoin',
      region: 'us-west-2',
    }
  },
  /**
   * @param {import('sst/constructs').App} app
   */
  stacks(app) {
    app.setDefaultFunctionProps({
      runtime: 'nodejs18.x',
      architecture: 'arm_64',
      environment: {
        NODE_OPTIONS: '--enable-source-maps',
      },
      nodejs: {
        format: 'esm',
        sourcemap: true
      }
    })

    app
      .stack(ApiStack)
      .stack(ProcessorStack)
    
    // tags let us discover all the aws resource costs incurred by this app
    // see: https://docs.sst.dev/advanced/tagging-resources
    Tags.of(app).add('Project', 'spade-proxy')
    Tags.of(app).add('Repository', 'https://github.com/web3-storage/spade-proxy')
    Tags.of(app).add('Environment', `${app.stage}`)
    Tags.of(app).add('ManagedBy', 'SST')
  }
}
