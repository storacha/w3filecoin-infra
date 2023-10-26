import * as Sentry from '@sentry/serverless'
import { Table } from 'sst/node/table'

import { createClient as createSpadeOracleStoreClient } from '@w3filecoin/core/src/store/spade-oracle-store.js'
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
    spadeOracleStoreBucketName,
    spadeOracleStoreBucketRegion,
    dealStoreTableName,
    dealStoreTableRegion,
    spadeOracleUrl
  } = getLambdaEnv()

  const spadeOracleStore = createSpadeOracleStoreClient({
    region: spadeOracleStoreBucketRegion
  }, {
    name: spadeOracleStoreBucketName
  })
  const dealStore = createDealStoreClient({
    region: dealStoreTableRegion
  }, {
    tableName: dealStoreTableName.tableName
  })

  const { ok, error } = await spadeOracleSyncTick({
    dealStore,
    spadeOracleStore,
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
    body: ok
  }
}

/**
 * Get Env validating it is set.
 */
function getLambdaEnv () {
  return {
    spadeOracleStoreBucketName: mustGetEnv('SPADE_ORACLE_STORE_BUCKET_NAME'),
    spadeOracleStoreBucketRegion: mustGetEnv('SPADE_ORACLE_STORE_REGION'),
    dealStoreTableName: Table['deal-tracker-deal-store'],
    dealStoreTableRegion: mustGetEnv('AWS_REGION'),
    spadeOracleUrl: mustGetEnv('SPADE_ORACLE_URL'),
  }
}
