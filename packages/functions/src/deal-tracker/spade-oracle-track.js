import * as Sentry from '@sentry/serverless'
import { Table } from 'sst/node/table'

import { createTableStoreClient } from '@w3filecoin/core/src/store/table-client.js'
import { encode as dealEncode, decode as dealDecode } from '@w3filecoin/core/src/data/deal.js'

import { mustGetEnv } from '../utils'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

export async function main() {
  // Construct context
  const {
    dealStoreTableName,
    dealStoreTableRegion,
    spadeOracleUrl
  } = getLambdaEnv()

  const dealStore = createTableStoreClient({
    region: dealStoreTableRegion
  }, {
    tableName: dealStoreTableName.tableName,
    encodeRecord: dealEncode.storeRecord,
    decodeRecord: dealDecode.storeRecord,
    encodeKey: dealEncode.storeKey
  })

  
}

/**
 * Get Env validating it is set.
 */
function getLambdaEnv () {
  return {
    dealStoreTableName: Table['deal-store'],
    dealStoreTableRegion: mustGetEnv('AWS_REGION'),
    spadeOracleUrl: mustGetEnv('SPADE_ORACLE_URL'),
  }
}