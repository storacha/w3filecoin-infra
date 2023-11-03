import { test } from './helpers/context.js'

import delay from 'delay'
import pRetry from 'p-retry'
import git from 'git-rev-sync'
import { randomCargo } from '@web3-storage/filecoin-api/test'
import { Aggregator } from '@web3-storage/filecoin-client'

import { getApiEndpoints, getStoreClients, getStage } from './helpers/deployment.js'
import { getClientConfig } from './helpers/client.js'
import { waitForStoreOperationOkResult } from './helpers/store.js'

test.before(t => {
  const api = getApiEndpoints()
  const store = getStoreClients()
  t.context = {
    api,
    store
  }
})

/**
 * @typedef {import('@web3-storage/data-segment').PieceLink} PieceLink
 */

test('GET /version', async t => {
  const stage = getStage()

  // `aggregator-api`
  const aggregatorApiResponse = await fetch(`${t.context.api.aggregator}/version`)
  t.is(aggregatorApiResponse.status, 200)

  const aggregatorApiBody = await aggregatorApiResponse.json()
  t.is(aggregatorApiBody.env, stage)
  t.is(aggregatorApiBody.commit, git.long('.'))

  // `dealer-api`
  const dealerApiResponse = await fetch(`${t.context.api.dealer}/version`)
  t.is(dealerApiResponse.status, 200)

  const dealerApiBody = await dealerApiResponse.json()
  t.is(dealerApiBody.env, stage)
  t.is(dealerApiBody.commit, git.long('.'))

  // `deal-tracker-api`
  const dealTrackerApiResponse = await fetch(`${t.context.api.tracker}/version`)
  t.is(dealTrackerApiResponse.status, 200)

  const dealTrackerApiBody = await dealTrackerApiResponse.json()
  t.is(dealTrackerApiBody.env, stage)
  t.is(dealTrackerApiBody.commit, git.long('.'))
})

// Integration tests mocking a storefront
// Integration tests that verifies full flow from `piece/offer` received by Aggregator, into a deal being done
test('POST /', async t => {
  // TODO: Mock a Storefront!

  // Client connection config for w3filecoin pipeline entrypoint, i.e. API Storefront relies on
  const { invocationConfig, connection } = await getClientConfig(new URL(t.context.api.aggregator))
  const group = invocationConfig.with

  // Create random pieces to add
  const pieces = await randomCargo(10, 1024)

  console.log('wait for all pieces to be offered for aggregation...')
  // Offer all pieces to be aggregated and added to a deal
  const pieceOfferResponses = await Promise.all(
    pieces.map(p => Aggregator.pieceOffer(invocationConfig, p.link, group, { connection }))
  )

  // All pieces succeeded to be queued
  t.is(
    pieceOfferResponses.reduce((accum, res) => {
      if (res.out.ok) {
        accum += 1
      }
      return accum
    }, 0),
    pieces.length
  )
  console.log('all pieces were successfully offered')

  // wait for piece-store entry to exist given it is propagated with a queue message to be added
  await delay(5e3)

  console.log('wait for all pieces to be verified and stored...')
  await Promise.all(
    pieces.map(p => pRetry(async () => {
      const storedPiece = await waitForStoreOperationOkResult(
        () => t.context.store.aggregator.pieceStore.get({ piece: p.link, group }),
        (res) => Boolean(res.ok)
      )
      if (!storedPiece.ok) {
        throw new Error('piece not available')
      }
      // Validate piece entry content
      t.truthy(storedPiece.ok.piece.equals(p.link))
      t.is(storedPiece.ok.group, invocationConfig.with)
      t.is(storedPiece.ok.status, 'offered')
      t.truthy(storedPiece.ok.insertedAt)
      t.truthy(storedPiece.ok.updatedAt)

      return storedPiece
    }))
  )
  console.log('all pieces were correctly stored')
  console.log(`wait for aggregate entries for group ${group} ...`)

  // wait for aggregate-store entry to exist given it is propagated with a queue message
  await delay(30e3)

  // Validate aggregates have at least a subset of pieces created here
  const groupAggregates = await waitForStoreOperationOkResult(
    () => t.context.store.aggregator.aggregateStore.query({ group }),
    (res) => Boolean(res.ok?.length)
  )
  if (!groupAggregates.ok) {
    throw new Error('group aggregate not available')
  }

  console.log(`wait for buffers that have pieces for found aggregates ...`)
  const aggregatedBuffers = await Promise.all(groupAggregates.ok.map(aggregate =>
    waitForStoreOperationOkResult(
      () => t.context.store.aggregator.bufferStore.get(aggregate.buffer),
      (res) => Boolean(res.ok)
  )))

  if (aggregatedBuffers.find(r => r.error)) {
    throw new Error('buffer with the aggregate is not available')
  }

  /** @type {{piece: PieceLink, aggregate: PieceLink}[]} */
  // @ts-ignore
  const entriesInAggregatedBuffers = aggregatedBuffers.reduce((acc, aggregatedBuffer) => {
    if (!aggregatedBuffer.ok) {
      throw new Error('aggregate buffers could not be fetched')
    }
    const buffer = aggregatedBuffer.ok.buffer

    return [
      ...acc,
      ...buffer.pieces.map(p => ({
        piece: p.piece,
        aggregate: buffer.aggregate
      }))
    ]
  }, /** @type {{piece: PieceLink, aggregate: PieceLink}[]} */ ([]))

  const convergedEntry = entriesInAggregatedBuffers.find(aggregatedEntry => pieces.find(p => p.link.equals(aggregatedEntry.piece)))
  t.truthy(convergedEntry)

  // Verify inclusion stored
  console.log(`wait for inclusion records for pieces in aggregate ...`)
  const inclusionRecords = await Promise.all(entriesInAggregatedBuffers.map((aggregateEntry) =>
    waitForStoreOperationOkResult(
      () => t.context.store.aggregator.inclusionStore.get({ piece: aggregateEntry.piece, aggregate: aggregateEntry.aggregate }),
      (res) => Boolean(res.ok)
    )
  ))
  if (inclusionRecords.find(r => r.error)) {
    throw new Error('one or more inclusion records is not available')
  }

  // verify dealer aggregate store
  console.log(`wait for dealer to receive aggregate offer ...`)
  const offeredAggregatesForDeal = await Promise.all(groupAggregates.ok.map(aggregate =>
    waitForStoreOperationOkResult(
      () => t.context.store.dealer.aggregateStore.get({ aggregate: aggregate.aggregate }),
      (res) => Boolean(res.ok)
  )))

  if (offeredAggregatesForDeal.find(r => r.error)) {
    throw new Error('buffer with the aggregate is not available')
  }

  // deals should be waiting resolution
  if (offeredAggregatesForDeal.find(r => r.ok?.status === 'accepted' || r.ok?.status === 'invalid')) {
    throw new Error('deals should be waiting resolution')
  }

  console.log(`wait for dealer to generate offers for broker ...`)
  // Verify deal offer store :)
  const offersForDeal = await Promise.all(offeredAggregatesForDeal.map(offeredAggregate =>
    waitForStoreOperationOkResult(
      () => t.context.store.dealer.offerStore.get(
        // @ts-ignore it is validated before that there are no errors
        getDealerOfferStoreKey(offeredAggregate.ok)
      ),
      (res) => Boolean(res.ok)
    )  
  ))

  if (offersForDeal.find(r => r.error)) {
    throw new Error('buffer with the aggregate is not available')
  }

  // Verify offers and order of pieces
  for (const offer of offersForDeal) {
    const bufferAggregate = aggregatedBuffers.find(ab => ab.ok?.buffer.aggregate?.equals(offer.ok.value.aggregate))

    for (let i = 0; i < offer.ok.value.pieces.length; i++) {
      t.truthy(bufferAggregate?.ok?.buffer.pieces[i].piece.equals(offer.ok.value.pieces[i]))
    }
  }

  // Set deal ready and check CRON outcome
  console.log(`wait for deal-tracker to have deals for aggregates ...`)
  const dealId = 1111
  const addedDeals = await Promise.all(offeredAggregatesForDeal.map(offeredAggregate => {
    if (!offeredAggregate.ok) {
      throw new Error('offered aggregate was not fetched')
    }
    const piece = offeredAggregate.ok.aggregate
    return waitForStoreOperationOkResult(
      () => t.context.store.tracker.dealStore.put({
        piece,
        provider: 'f0001',
        dealId,
        expirationEpoch: Date.now() + 10e9,
        insertedAt: (new Date()).toISOString(),
        source: 'testing'
      }),
      (res) => Boolean(res.ok)
    )
  }))

  if (addedDeals.find(r => r.error)) {
    throw new Error('could not write deals on deal tracker deal store')
  }

  // Trigger cron to update and issue receipts based on deals
  const callCronRes = await fetch(`${t.context.api.dealer}/cron`)
  t.true(callCronRes.ok)

  // Verify deals accepted
  console.log(`wait for deals to be marked as accepted ...`)
  const acceptedAggregatesForDeal = await Promise.all(groupAggregates.ok.map(aggregate =>
    waitForStoreOperationOkResult(
      () => t.context.store.dealer.aggregateStore.get({ aggregate: aggregate.aggregate }),
      (res) => Boolean(res.ok) && res.ok?.status === 'accepted'
  )))

  if (acceptedAggregatesForDeal.find(r => r.error)) {
    throw new Error('deals not found with accepted status')
  }

  // go through receipt chain
  console.log(`wait for receipt chain...`)
  const pieceOfferReceiptCid = (pieceOfferResponses.find(p => p.out.ok?.piece.equals(convergedEntry?.piece)))?.ran.link()
  if (!pieceOfferReceiptCid) {
    throw new Error('no cid for `piece/offer`')
  }

  console.log(`wait for piece/offer receipt...`)
  const pieceOfferReceipt = await waitForStoreOperationOkResult(
    () => t.context.store.storefront.receiptStore.get(pieceOfferReceiptCid),
    (res) => Boolean(res.ok)
  )
  const pieceAcceptReceiptCid = pieceOfferReceipt.ok?.fx.join?.link()
  if (!pieceAcceptReceiptCid) {
    throw new Error('piece/offer receipt has no effect for piece/accept')
  }
  // @ts-ignore
  t.truthy(pieceOfferReceipt.ok?.out.ok?.piece)

  console.log(`wait for piece/accept receipt...`)
  const pieceAcceptReceipt = await waitForStoreOperationOkResult(
    () => t.context.store.storefront.receiptStore.get(pieceAcceptReceiptCid),
    (res) => Boolean(res.ok)
  )
  const aggregateOfferReceiptCid = pieceAcceptReceipt.ok?.fx.join?.link()
  if (!aggregateOfferReceiptCid) {
    throw new Error('piece/accept receipt has no effect for aggregate/offer')
  }

  // @ts-ignore
  t.truthy(pieceAcceptReceipt.ok?.out.ok?.piece)
  // @ts-ignore
  t.truthy(pieceAcceptReceipt.ok?.out.ok?.aggregate)
  // @ts-ignore
  t.truthy(pieceAcceptReceipt.ok?.out.ok?.inclusion)

  console.log(`wait for aggregate/offer receipt...`)
  const aggregateOfferReceipt = await waitForStoreOperationOkResult(
    () => t.context.store.storefront.receiptStore.get(aggregateOfferReceiptCid),
    (res) => Boolean(res.ok)
  )
  const aggregateAcceptReceiptCid = aggregateOfferReceipt.ok?.fx.join?.link()
  if (!aggregateAcceptReceiptCid) {
    throw new Error('aggregate/offer receipt has no effect for aggregate/accept')
  }

  console.log(`wait for aggregate/accept receipt...`)
  const aggregateAcceptReceipt = await waitForStoreOperationOkResult(
    () => t.context.store.storefront.receiptStore.get(aggregateAcceptReceiptCid),
    (res) => Boolean(res.ok)
  )

  // @ts-ignore
  t.is(aggregateAcceptReceipt.ok?.out.ok.dataSource.dealID, dealId)
  console.log('deal proved')
})

/**
 * 
 * @param {import('@web3-storage/filecoin-api/dealer/api').AggregateRecord} record 
 */
function getDealerOfferStoreKey (record) {
  return `${new Date(
    record.insertedAt
  ).toISOString()} ${record.aggregate.toString()}.json`
}