import * as Sentry from '@sentry/serverless'
import { Table } from 'sst/node/table'

import { createClient as createDealArchiveStoreClient } from '@w3filecoin/core/src/store/deal-archive-store.js'
import { createClient as createDealStoreClient } from '@w3filecoin/core/src/store/deal-store.js'
import { spadeOracleSyncTick } from '@w3filecoin/core/src/deal-tracker/spade-oracle-sync-tick.js'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

export async function main() {
  // Construct context
  const {
    dealArchiveStoreBucketName,
    dealArchiveStoreBucketRegion,
    dealStoreTableName,
    dealStoreTableRegion,
    spadeOracleUrl
  } = getLambdaEnv()

  const dealArchiveStore = createDealArchiveStoreClient({
    region: dealArchiveStoreBucketRegion
  }, {
    name: dealArchiveStoreBucketName
  })
  const dealStore = createDealStoreClient({
    region: dealStoreTableRegion
  }, {
    tableName: dealStoreTableName.tableName
  })

  const { error } = await spadeOracleSyncTick({
    dealStore,
    dealArchiveStore,
    spadeOracleUrl: new URL(spadeOracleUrl)
  })

  if (error) {
    return {
      statusCode: 500,
      body: error.message
    }
  }

  return {
    statusCode: 200,
  }
}

/**
 * Get Env validating it is set.
 */
function getLambdaEnv () {
  return {
    dealArchiveStoreBucketName: mustGetEnv('DEAL_ARCHIVE_STORE_BUCKET_NAME'),
    dealArchiveStoreBucketRegion: mustGetEnv('DEAL_ARCHIVE_STORE_REGION'),
    dealStoreTableName: Table['deal-tracker-deal-store'],
    dealStoreTableRegion: mustGetEnv('AWS_REGION'),
    spadeOracleUrl: mustGetEnv('SPADE_ORACLE_URL'),
  }
}
