import { createRequire } from 'module'
import fs from 'fs'
import path from 'path'

import { S3Client } from '@aws-sdk/client-s3'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'

// Aggregator stores
import { createClient as createAggregatorAggregateClient } from '@w3filecoin/core/src/store/aggregator-aggregate-store.js'
import { createClient as createAggregatorBufferClient } from '@w3filecoin/core/src/store/aggregator-buffer-store.js'
import { createClient as createAggregatorInclusionProofClient } from '@w3filecoin/core/src/store/aggregator-inclusion-proof-store.js'
import { createClient as createAggregatorInclusionClient } from '@w3filecoin/core/src/store/aggregator-inclusion-store.js'
import { createClient as createAggregatorPieceClient } from '@w3filecoin/core/src/store/aggregator-piece-store.js'

// Dealer stores
import { createClient as createDealerAggregateStore } from '@w3filecoin/core/src/store/dealer-aggregate-store.js'
import { createClient as createDealerOfferStore } from '@w3filecoin/core/src/store/dealer-offer-store.js'

// DealTracker stores
import { createClient as createDealTrackerDealStoreClient } from '@w3filecoin/core/src/store/deal-store.js'

// Storefront stores
import { useReceiptStore } from './receipt-store.js'

export function getStage () {
  const stage = process.env.SST_STAGE || process.env.SEED_STAGE_NAME
  if (stage) {
    return stage
  }

  const f = fs.readFileSync(path.join(
    process.cwd(),
    '.sst/stage'
  ))

  return f.toString()
}

export const getStackName = () => {
  const stage = getStage()
  return `${stage}-w3filecoin`
}

export const getAwsRegion = () => {
  // CI/CD deployment
  if (process.env.SEED_APP_NAME) {
    return 'us-east-2'
  }

  return 'us-west-2'
}

/**
 * @param {string} name
 */
const getTableName = (name) => {
  const stage = getStage()
  return `${stage}-w3filecoin-${name}`
}

/**
 * @param {string} name
 */
const getBucketName = (name) => {
  const stage = getStage()
  return `${stage}-w3filecoin-${name}-0`
}

export const getStoreClients = () => {
  const region = getAwsRegion()
  const s3client = new S3Client({
    region
  })
  const s3clientInfra = new S3Client({
    region: 'us-east-2'
  })
  const dynamoClient = new DynamoDBClient({
    region,
    endpoint: `https://dynamodb.${region}.amazonaws.com`
  })

  const inclusionProofStore = createAggregatorInclusionProofClient(s3client,
    { name: getBucketName('aggregator-inclusion-proof-store') }
  )

  const invocationBucketName = 'invocation-store-staging-0'
  const workflowBucketName = 'workflow-store-staging-0'

  return {
    storefront: {
      receiptStore: useReceiptStore(s3clientInfra, invocationBucketName, workflowBucketName)
    },
    aggregator: {
      aggregateStore: createAggregatorAggregateClient(dynamoClient,
        { tableName: getTableName('aggregator-aggregate-store') }
      ),
      bufferStore: createAggregatorBufferClient(s3client,
        { name: getBucketName('aggregator-buffer-store') }
      ),
      pieceStore: createAggregatorPieceClient(dynamoClient,
        { tableName: getTableName('aggregator-piece-store') }
      ),
      inclusionStore: createAggregatorInclusionClient(dynamoClient,
        { tableName: getTableName('aggregator-inclusion-store'), inclusionProofStore }
      ),
      inclusionProofStore
    },
    dealer: {
      aggregateStore: createDealerAggregateStore(dynamoClient,
        { tableName: getTableName('dealer-aggregate-store') }
      ),
      offerStore: createDealerOfferStore(s3client,
        { name: getBucketName('dealer-offer-store') }
      )
    },
    tracker: {
      dealStore: createDealTrackerDealStoreClient(dynamoClient,
        { tableName: getTableName('deal-tracker-deal-store-v1') }
      )
    }
  }
}

export const getApiEndpoints = () => {
  const stage = getStage()

  // CI/CD deployment
  if (process.env.SEED_APP_NAME) {
    return {
      aggregator: `https://${stage}.aggregator.web3.storage`,
      dealer: `https://${stage}.dealer.web3.storage`,
      tracker: `https://${stage}.tracker.web3.storage`
    }
  }

  const require = createRequire(import.meta.url)
  const testEnv = require(path.join(
    process.cwd(),
    '.sst/outputs.json'
  ))

  // Get API Stack endpoints
  const id = 'ApiStack'
  return {
    aggregator: testEnv[`${getStackName()}-${id}`].AggregatorApiEndpoint,
    dealer: testEnv[`${getStackName()}-${id}`].DealerApiEndpoint,
    tracker: testEnv[`${getStackName()}-${id}`].DealTrackerApiEndpoint
  }
}
