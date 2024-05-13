import prettyMilliseconds from 'pretty-ms'

/**
 * @typedef {import('@web3-storage/data-segment').PieceLink} PieceLink
 * @typedef {import('@web3-storage/filecoin-api/aggregator/api').AggregateStore} AggregatorAggregateStore
 * @typedef {import('@web3-storage/filecoin-api/dealer/api').AggregateStore} DealerAggregateStore
 * @typedef {import('@web3-storage/filecoin-api/dealer/api').AggregateRecord} AggregateRecord
 * 
 * @typedef {object} MonitorContext
 * @property {AggregatorAggregateStore} context.aggregatorAggregateStore
 * @property {DealerAggregateStore} context.dealerAggregateStore
 * @property {number} context.minPieceCriticalThresholdMs
 * @property {number} context.minPieceWarnThresholdMs
 * @property {number} context.aggregateMonitorThresholdMs
 * @property {URL} context.monitoringNotificationsUrl
 * 
 * @typedef {object} Alert
 * @property {PieceLink} aggregate
 * @property {number} duration
 * @property {string} severity
 */

/**
 * On CRON tick, get aggregates without deals, and verify if thresholds are met to notify monitoring.
 * 
 * @param {MonitorContext} context
 */
export async function dealMonitorAlertTick (context) {
  // Get offered deals pending approval/rejection
  const offeredAggregates = await context.dealerAggregateStore.query({
    status: 'offered',
  })
  if (offeredAggregates.error) {
    return {
      error: offeredAggregates.error,
    }
  }

  // Get offered aggregates to monitor
  const offeredAggregatesToMonitor = []
  const currentTime = Date.now()
  for (const offeredAggregate of offeredAggregates.ok) {
    const offerTime = (new Date(offeredAggregate.insertedAt)).getTime()
    // Monitor if current time is bigger than offer time + monitor threshold
    if (currentTime > (offerTime + context.aggregateMonitorThresholdMs)) {
      offeredAggregatesToMonitor.push(offeredAggregate)
    }
  }

  // Get aggregates duration
  const monitoredAggregatesResponse = await Promise.all(
    offeredAggregatesToMonitor.map(aggregate => monitorAggregate(aggregate, context))
  )
  // Fail if any failed to get information
  const monitoredAggregatesErrorResponse = monitoredAggregatesResponse.find(r => r?.error)
  if (monitoredAggregatesErrorResponse) {
    return {
      error: monitoredAggregatesErrorResponse.error
    }
  }

  const alerts = /** @typedef {Alert[]} */ ([])
  // Verify if monitored aggregates should create notifications
  for (const res of monitoredAggregatesResponse) {
    // @ts-ignore if not ok, should have failed before
    const duration = /** @type {number} */ (res.ok?.duration)
    // @ts-ignore if not ok, should have failed before
    const aggregate = /** @type {import('@web3-storage/data-segment').PieceLink} */ (res.ok?.aggregate)

    if (duration > context.minPieceCriticalThresholdMs) {
      alerts.push({
        aggregate,
        duration,
        severity: 'critical'
      })
    } else if (duration > context.minPieceWarnThresholdMs) {
      alerts.push({
        aggregate,
        duration,
        severity: 'warn'
      })
    }
  }

  if (!alerts.length) {
    return {
      ok: {
        alerts
      }
    }
  }

  // Send alerts
  const alertPayload = getAlertPayload(alerts)
  const alertResponse = await fetch(
    context.monitoringNotificationsUrl,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(alertPayload)
    }
  )
  if (!alertResponse.ok) {
    return {
      error: new Error(`failed to send alert to ${context.monitoringNotificationsUrl} with ${alerts.length}`)
    }
  }

  return {
    ok: {
      alerts
    }
  }
}

/**
 * @param {AggregateRecord} aggregateRecord
 * @param {MonitorContext} context
 */
async function monitorAggregate (aggregateRecord, context) {
  const getAggregateInfo = await context.aggregatorAggregateStore.get({
    aggregate: aggregateRecord.aggregate
  })
  if (getAggregateInfo.error) {
    return {
      error: getAggregateInfo.error
    }
  }

  // Get aggregate current duration
  const currentTime = Date.now()
  const offerTime = (new Date(getAggregateInfo.ok.minPieceInsertedAt)).getTime()

  return {
    ok: {
      aggregate: aggregateRecord.aggregate,
      duration: currentTime - offerTime
    }
  }
}

/**
 * Construct alert based on payload from Grafana Alerting.
 *
 * @see https://grafana.com/docs/oncall/latest/integrations/grafana-alerting/
 * @see https://prometheus.io/docs/alerting/latest/notifications/#data
 *
 * @param {Alert[]} alerts
 */
function getAlertPayload (alerts) {
  return {
    alerts: alerts.map(a => ({
      labels: {
        aggregate: a.aggregate.toString(),
        duration: prettyMilliseconds(a.duration),
        severity: a.severity,
      },
      status: 'firing',
      fingerprint: a.aggregate.toString()
    })),
    status: 'firing',
    version: '4',
    // eslint-disable-next-line no-useless-escape
    groupKey: '{}:{alertname=\"FilecoinDealDelay\"}',
    receiver: 'combo',
    groupLabels: {
      alertname: 'FilecoinDealDelay'
    },
    commonLabels: {
      job: 'deal-monitor-alert',
      group: 'production',
      alertname: 'FilecoinDealDelay'
    }
  }
}
