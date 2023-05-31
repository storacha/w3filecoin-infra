import { pipe } from 'it-pipe'
import { batch } from 'streaming-iterables'
import { CID } from 'multiformats/cid'

import { MAX_BATCH_GET_ITEMS } from '../tables/constants.js'
import { createCarTable } from '../tables/car.js'
import { createFerryTable } from '../tables/ferry.js'
import { createAggregateService } from './aggregate-service.js'

/**
 * @typedef {import('../types.js').FerryOpts} FerryOpts
 * @typedef {import('../types.js').CarItem} CarItem
 * @typedef {import('../types.js').CarItemFerry} CarItemFerry
 *
 * @typedef {object} FerryCtx
 * @property {string} region
 * @property {string} tableName
 * @property {FerryOpts} [options]
 * 
 * @typedef {object} CarTableCtx
 * @property {string} region
 * @property {string} tableName
 * @property {import('../types.js').CarOpts} [options]
 */

/**
 * Add cars to a loading ferry.
 *
 * @param {CarItemFerry[]} cars
 * @param {FerryCtx} ferryCtx
 */
export async function addCarsToFerry (cars, ferryCtx) {
  const ferryTable = createFerryTable(ferryCtx.region, ferryCtx.tableName, ferryCtx.options)

  const ferryId = await ferryTable.getFerryLoading()
  await ferryTable.addCargo(ferryId, cars)

  return {
    id: ferryId
  }
}

/**
 * Sets current Ferry as ready if not previously done
 *
 * @param {string} ferryId
 * @param {FerryCtx} ferryCtx
 */
export async function setFerryAsReady (ferryId, ferryCtx) {
  const ferryTable = createFerryTable(ferryCtx.region, ferryCtx.tableName, ferryCtx.options)

  // Update state of ferry to ready
  try {
    await ferryTable.setAsReady(ferryId)
  } catch (/** @type {any} */ error) {
    // If error is for condition we can safely ignore it given this was changed in a concurrent operation
    if (error.name !== 'ConditionalCheckFailedException') {
      throw error
    }
  }
}

/**
 * Sets current Ferry offer.
 *
 * @param {string} ferryId
 * @param {object} ctx
 * @param {FerryCtx} ctx.car
 * @param {FerryCtx} ctx.ferry
 * @param {import('../types.js').StorefrontSignerCtx} ctx.storefront
 * @param {import('@ucanto/principal/ed25519').ConnectionView<any>} ctx.aggregationServiceConnection
 */
export async function setFerryOffer (ferryId, ctx) {
  const carTable = createCarTable(ctx.car.region, ctx.car.tableName, ctx.car.options)
  const ferryTable = createFerryTable(ctx.ferry.region, ctx.ferry.tableName, ctx.ferry.options)
  const aggregateService = await createAggregateService(ctx.storefront, ctx.aggregationServiceConnection)

  // Create Offer
  /** @type {CarItem[]} */
  const offers = await pipe(
    ferryTable.getCargo(ferryId, { limit: MAX_BATCH_GET_ITEMS }),
    batch(MAX_BATCH_GET_ITEMS),
    /**
     * @param {AsyncGenerator<CarItemFerry[], any, unknown> | Generator<CarItemFerry[], any, unknown>} source 
     */
    // @ts-expect-error type not inferred
    async function (source) {
      /** @type {CarItemFerry[]} */
      const cars = []
      for await (const items of source) {
        const pageCars = await carTable.batchGet(items)
        for (const car of pageCars) {
          cars.push(car)
        }
      }

      return cars
    }
  )

  // Send offer
  const nOffers = offers.map(offer => ({
    ...offer,
    link: CID.parse(offer.link).link()
  }))
  // @ts-expect-error CID versions
  await aggregateService.offer(nOffers)

  // Update state of ferry to ready
  try {
    await ferryTable.setAsDealPending(ferryId)
  } catch (/** @type {any} */ error) {
    // If error is for condition we can safely ignore it given this was changed in a concurrent operation
    if (error.name !== 'ConditionalCheckFailedException') {
      throw error
    }
  }
}

/**
 * Sets current Ferry as deal processed
 *
 * @param {string} ferryId
 * @param {string} commP
 * @param {FerryCtx} ferryCtx
 */
export async function setFerryAsProcessed (ferryId, commP, ferryCtx) {
  const ferryTable = createFerryTable(ferryCtx.region, ferryCtx.tableName, ferryCtx.options)

  // Update state of ferry to deal processed
  try {
    await ferryTable.setAsDealProcessed(ferryId, commP)
  } catch (/** @type {any} */ error) {
    // If error is for condition we can safely ignore it given this was changed in a concurrent operation
    if (error.name !== 'ConditionalCheckFailedException') {
      throw error
    }
  }
}
