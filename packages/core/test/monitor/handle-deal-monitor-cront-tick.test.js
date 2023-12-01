import delay from 'delay'
import * as Signer from '@ucanto/principal/ed25519'
import { randomAggregate } from '@web3-storage/filecoin-api/test'
import { CBOR } from '@ucanto/server'

import { createClient as createAggregatorAggregateStoreClient } from '../../src/store/aggregator-aggregate-store.js'
import { createClient as createDealerAggregateStoreClient } from '../../src/store/dealer-aggregate-store.js'
import { dealerAggregateStoreTableProps, aggregatorAggregateStoreTableProps } from '../../src/store/index.js'

import * as dealMonitorAlertTick from '../../src/monitor/deal-monitor-alert-tick.js'

import { testStore as test } from '../helpers/context.js'
import { createDynamodDb, createTable } from '../helpers/resources.js'

test.before(async (t) => {
  const { client: dynamoClient, stop: dynamoStop} = await createDynamodDb()

  Object.assign(t.context, {
    dynamoClient,
    stop: async () => {
      await dynamoStop()
    }
  })
})

test.after(async t => {
  await t.context.stop()
  await delay(1000)
})

test('handles deal monitor tick without aggregates available', async t => {
  const context = await getContext(t.context)

  const tickRes = await dealMonitorAlertTick.dealMonitorAlertTick({
    ...context,
    minPieceCriticalThresholdMs: 0,
    minPieceWarnThresholdMs: 0,
    aggregateMonitorThresholdMs: 0
  })

  t.assert(tickRes.ok)
  t.is(tickRes.ok?.alerts.length, 0)
})

test('handles deal monitor tick with aggregates in warn type', async t => {
  const context = await getContext(t.context)
  const storefront = await Signer.generate()
  const group = storefront.did()
  const { pieces, aggregate } = await randomAggregate(10, 128)
  const threshold = 1000
  const pieceInsertTime = Date.now() - threshold
  const buffer = {
    pieces: pieces.map((p) => ({
      piece: p.link,
      insertedAt: new Date(
        pieceInsertTime
      ).toISOString(),
      policy: 0,
    })),
    group,
  }
  const block = await CBOR.write(buffer)
  // Store aggregate record into store
  const offer = pieces.map((p) => p.link)
  const piecesBlock = await CBOR.write(offer)

  // Store aggregate in aggregator
  const aggregatePutRes = await context.aggregatorAggregateStore.put({
    aggregate: aggregate.link,
    pieces: piecesBlock.cid,
    buffer: block.cid,
    group,
    insertedAt: new Date().toISOString(),
    minPieceInsertedAt: new Date().toISOString(),
  })
  t.assert(aggregatePutRes.ok)

  // Propagate aggregate to dealer
  const putRes = await context.dealerAggregateStore.put({
    aggregate: aggregate.link,
    pieces: piecesBlock.cid,
    status: 'offered',
    insertedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  })
  t.assert(putRes.ok)

  const tickRes = await dealMonitorAlertTick.dealMonitorAlertTick({
    ...context,
    // do not wait
    minPieceCriticalThresholdMs: threshold * 10,
    minPieceWarnThresholdMs: 0,
    aggregateMonitorThresholdMs: threshold
  })

  t.assert(tickRes.ok)
  t.is(tickRes.ok?.alerts.length, 1)
  t.is(tickRes.ok?.alerts[0].severity, 'warn')
  t.assert(tickRes.ok?.alerts[0].duration)
  t.assert(tickRes.ok?.alerts[0].aggregate.equals(aggregate.link))
})

test('handles deal monitor tick with aggregates in critical type', async t => {
  const context = await getContext(t.context)
  const storefront = await Signer.generate()
  const group = storefront.did()
  const { pieces, aggregate } = await randomAggregate(10, 128)
  const threshold = 1000
  const pieceInsertTime = Date.now() - threshold
  const buffer = {
    pieces: pieces.map((p) => ({
      piece: p.link,
      insertedAt: new Date(
        pieceInsertTime
      ).toISOString(),
      policy: 0,
    })),
    group,
  }
  const block = await CBOR.write(buffer)
  // Store aggregate record into store
  const offer = pieces.map((p) => p.link)
  const piecesBlock = await CBOR.write(offer)

  // Store aggregate in aggregator
  const aggregatePutRes = await context.aggregatorAggregateStore.put({
    aggregate: aggregate.link,
    pieces: piecesBlock.cid,
    buffer: block.cid,
    group,
    insertedAt: new Date().toISOString(),
    minPieceInsertedAt: new Date().toISOString(),
  })
  t.assert(aggregatePutRes.ok)

  // Propagate aggregate to dealer
  const putRes = await context.dealerAggregateStore.put({
    aggregate: aggregate.link,
    pieces: piecesBlock.cid,
    status: 'offered',
    insertedAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  })
  t.assert(putRes.ok)

  const tickRes = await dealMonitorAlertTick.dealMonitorAlertTick({
    ...context,
    // should do critical check first
    minPieceCriticalThresholdMs: 0,
    minPieceWarnThresholdMs: 0,
    aggregateMonitorThresholdMs: threshold
  })

  t.assert(tickRes.ok)
  t.is(tickRes.ok?.alerts.length, 1)
  t.is(tickRes.ok?.alerts[0].severity, 'critical')
  t.assert(tickRes.ok?.alerts[0].duration)
  t.assert(tickRes.ok?.alerts[0].aggregate.equals(aggregate.link))
})

/**
 * @param {import('../helpers/context.js').DbContext} context
 */
async function getContext (context) {
  const { dynamoClient } = context
  const aggregatorAggregateStoreTableName = await createTable(dynamoClient, aggregatorAggregateStoreTableProps)
  const dealerAggregateStoreTableName = await createTable(dynamoClient, dealerAggregateStoreTableProps)

  return {
    aggregatorAggregateStore: createAggregatorAggregateStoreClient(dynamoClient, {
      tableName: aggregatorAggregateStoreTableName
    }),
    dealerAggregateStore: createDealerAggregateStoreClient(dynamoClient, {
      tableName: dealerAggregateStoreTableName
    }),
    monitoringNotificationsUrl: new URL(`http://127.0.0.1:${process.env.PORT || 9001}`)
  }
}
