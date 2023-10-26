import { Cron, use } from 'sst/constructs'

import { DataStack } from './data-stack.js'
import {
  setupSentry,
  getDealTrackerEnv,
  getResourceName
} from './config.js'

/**
 * @param {import('sst/constructs').StackContext} properties
 */
export function DealTrackerStack({ stack, app }) {
  const {
    SPADE_ORACLE_URL
  } = getDealTrackerEnv()

  // Setup app monitoring with Sentry
  setupSentry(app, stack)

  const {
    dealTrackerDealStoreTable,
    dealTrackerSpaceOracleStoreBucket
  } = use(DataStack)

  /**
   * CRON to track deals resolution from Spade Oracle
   */
  const spadeOracleCronName = getResourceName('spade-oracle-sync-cron', stack.stage)
  const spadeOracleCron = new Cron(stack, spadeOracleCronName, {
    // Spade updates each hour
    schedule: 'rate(1 hour)',
    job: {
      function: {
        handler: 'packages/functions/src/deal-tracker/spade-oracle-sync-tick.main',
        memorySize: '1 GB',
        environment: {
          SPADE_ORACLE_URL,
          SPADE_ORACLE_STORE_BUCKET_NAME: dealTrackerSpaceOracleStoreBucket.bucketName,
          SPADE_ORACLE_STORE_REGION: stack.region,
        },
        bind: [
          dealTrackerDealStoreTable
        ],
        permissions: [
          dealTrackerSpaceOracleStoreBucket
        ]
      }
    }
  })

  return {
    spadeOracleCron
  }
}
