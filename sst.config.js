import { Tags } from 'aws-cdk-lib';

import { DataStack } from './stacks/data-stack.js'
import { DealTrackerStack } from './stacks/deal-tracker-stack.js'
import { DealerStack } from './stacks/dealer-stack.js'
import { ApiStack } from './stacks/api-stack.js'
import { AggregatorStack } from './stacks/aggregator-stack.js'

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
      .stack(DataStack)
      .stack(DealTrackerStack)
      .stack(AggregatorStack)
      .stack(ApiStack)
      .stack(DealerStack)
    
    // tags let us discover all the aws resource costs incurred by this app
    // see: https://docs.sst.dev/advanced/tagging-resources
    Tags.of(app).add('Project', 'w3filecoin')
    Tags.of(app).add('Repository', 'https://github.com/web3-storage/w3filecoin-infra')
    Tags.of(app).add('Environment', `${app.stage}`)
    Tags.of(app).add('ManagedBy', 'SST')
  }
}
