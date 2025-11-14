import * as Sentry from '@sentry/serverless'
import { Table } from 'sst/node/table'

// store clients
import { createClient as createAggregatorAggregateStoreClient } from '@w3filecoin/core/src/store/aggregator-aggregate-store.js'
import { createClient as createDealerAggregateStoreClient } from '@w3filecoin/core/src/store/dealer-aggregate-store.js'

import { dealMonitorAlertTick } from '@w3filecoin/core/src/monitor/deal-monitor-alert-tick.js'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 0
})

async function handleEvent () {
  const {
    aggregatorAggregateStoreTableName,
    aggregatorAggregateStoreTableRegion,
    dealerAggregateStoreTableName,
    dealerAggregateStoreTableRegion,
    minPieceCriticalThresholdMs,
    minPieceWarnThresholdMs,
    aggregateMonitorThresholdMs,
    monitoringNotificationsUrl
  } = getEnv()

  // stores
  const aggregatorAggregateStore = createAggregatorAggregateStoreClient(
    { region: aggregatorAggregateStoreTableRegion },
    { tableName: aggregatorAggregateStoreTableName.tableName }
  )
  const dealerAggregateStore = createDealerAggregateStoreClient({
    region: dealerAggregateStoreTableRegion
  }, {
    tableName: dealerAggregateStoreTableName.tableName
  })

  const { error } = await dealMonitorAlertTick({
    aggregatorAggregateStore,
    dealerAggregateStore,
    minPieceCriticalThresholdMs,
    minPieceWarnThresholdMs,
    aggregateMonitorThresholdMs,
    monitoringNotificationsUrl
  })

  if (error) {
    console.error(error)
    return {
      statusCode: 500,
      body: error.message
    }
  }

  return {
    statusCode: 200
  }
}

/**
 * Get Env validating it is set.
 */
function getEnv () {
  return {
    aggregatorAggregateStoreTableName: Table['aggregator-aggregate-store'],
    aggregatorAggregateStoreTableRegion: mustGetEnv('AWS_REGION'),
    dealerAggregateStoreTableName: Table['dealer-aggregate-store'],
    dealerAggregateStoreTableRegion: mustGetEnv('AWS_REGION'),
    minPieceCriticalThresholdMs: Number.parseInt(mustGetEnv('MIN_PIECE_CRITICAL_THRESHOLD_MS')),
    minPieceWarnThresholdMs: Number.parseInt(mustGetEnv('MIN_PIECE_WARN_THRESHOLD_MS')),
    aggregateMonitorThresholdMs: Number.parseInt(mustGetEnv('AGGREGATE_MONITOR_THRESHOLD_MS')),
    monitoringNotificationsUrl: new URL(mustGetEnv('MONITORING_NOTIFICATIONS_ENDPOINT'))
  }
}

export const main = Sentry.AWSLambda.wrapHandler(handleEvent)
