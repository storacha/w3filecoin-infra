import { test } from '../helpers/context.js'

import { useCargoTable, STATE as CARGO_STATE } from '../../src/table/cargo.js'
import { useFerryTable } from '../../src/table/ferry.js'

import { createDatabase } from '../helpers/resources.js'
import { getCargo } from '../helpers/cargo.js'

test.beforeEach(async (t) => {
  Object.assign(t.context, {
    dbClient: (await createDatabase()).client,
  })
})

test('can insert to cargo table', async t => {
  const { dbClient } = t.context
  const cargoTable = useCargoTable(dbClient)
  const cargoItems = await getCargo(10)

  await Promise.all(
    cargoItems.map(cargo => cargoTable.insert(cargo))
  )

  const queuedItems = await cargoTable.selectByState(CARGO_STATE.QUEUED)
  if (!queuedItems.ok) {
    throw new Error('could not get cargo items')
  }
  t.is(cargoItems.length, queuedItems.ok.length)
  for (const cargo of queuedItems.ok) {
    const index = cargoItems.findIndex(i => i.link.equals(cargo.link))
    t.truthy(cargo.link.equals(cargoItems[index].link))
    t.truthy(cargo.carLink.equals(cargoItems[index].carLink))
    t.is(cargo.state, CARGO_STATE.QUEUED)
    t.is(cargo.size, cargoItems[index].size)
    t.falsy(cargo.ferryLink)
    t.falsy(cargo.ferryFailedCode)
  }
})

test('errors trying to insert already existing cargo', async t => {
  const { dbClient } = t.context
  const cargoTable = useCargoTable(dbClient)
  const cargoItems = await getCargo(10)

  const res = await Promise.all(
    cargoItems.map(cargo => cargoTable.insert(cargo))
  )
  t.falsy(res.find(item => item.error))

  const { error } = await cargoTable.insert(cargoItems[0])
  t.truthy(error)
})

test('can update cargo state to offering', async t => {
  const { dbClient } = t.context
  const cargoTable = useCargoTable(dbClient)
  const ferryTable = useFerryTable(dbClient)
  const cargoItems = await getCargo(10)

  await Promise.all(
    cargoItems.map(cargo => cargoTable.insert(cargo))
  )
  // Get fake ferry link
  // TODO: get proper aggregate link and size
  const ferryItem = {
    link: cargoItems[0].link,
    size: cargoItems.reduce((accum, item) => item.size + accum, 0)
  }
  const cargoItemsToOffering = cargoItems.map(i => i.link)

  await ferryTable.insert(ferryItem, cargoItemsToOffering)
  const { error } = await cargoTable.updateCargoOffering(cargoItemsToOffering, ferryItem.link)
  t.falsy(error)

  const queuedItems = await cargoTable.selectByState(CARGO_STATE.QUEUED)
  const offeringItems = await cargoTable.selectByState(CARGO_STATE.OFFERING)
  t.is(queuedItems.ok?.length, 0)
  t.is(offeringItems.ok?.length, cargoItems.length)

  if (!offeringItems.ok) {
    throw new Error('could not get cargo items')
  }

  // Verify ferry link
  for (const offer of offeringItems.ok) {
    t.truthy(offer.ferryLink?.equals(ferryItem.link))
  }
})

test('can update cargo state to succeed', async t => {
  const { dbClient } = t.context
  const cargoTable = useCargoTable(dbClient)
  const ferryTable = useFerryTable(dbClient)
  const cargoItems = await getCargo(10)

  await Promise.all(
    cargoItems.map(cargo => cargoTable.insert(cargo))
  )
  // Get fake ferry link
  // TODO: get proper aggregate link and size
  const ferryItem = {
    link: cargoItems[0].link,
    size: cargoItems.reduce((accum, item) => item.size + accum, 0)
  }
  const cargoItemsToOffering = cargoItems.map(i => i.link)

  await ferryTable.insert(ferryItem, cargoItemsToOffering)
  await cargoTable.updateCargoOffering(cargoItemsToOffering, ferryItem.link)
  const { error } = await cargoTable.updateCargoSuccess(ferryItem.link)
  t.falsy(error)

  const queuedItems = await cargoTable.selectByState(CARGO_STATE.QUEUED)
  const offeringItems = await cargoTable.selectByState(CARGO_STATE.OFFERING)
  const succeedItems = await cargoTable.selectByState(CARGO_STATE.SUCCEED)
  const failedItems = await cargoTable.selectByState(CARGO_STATE.FAILED)
  t.is(queuedItems.ok?.length, 0)
  t.is(offeringItems.ok?.length, 0)
  t.is(succeedItems.ok?.length, cargoItems.length)
  t.is(failedItems.ok?.length, 0)
})

test('can update cargo failed or queued by providing a transaction client', async t => {
  const { dbClient } = t.context
  const cargoTable = useCargoTable(dbClient)
  const ferryTable = useFerryTable(dbClient)
  const cargoItems = await getCargo(10)

  await Promise.all(
    cargoItems.map(cargo => cargoTable.insert(cargo))
  )
  // Get fake ferry link
  // TODO: get proper aggregate link and size
  const ferryItem = {
    link: cargoItems[0].link,
    size: cargoItems.reduce((accum, item) => item.size + accum, 0)
  }
  const cargoItemsToOffering = cargoItems.map(i => i.link)
  const failedCargoItems = [{
    link: cargoItems[0].link,
    code: 'INVALID CARGO'
  }]

  await ferryTable.insert(ferryItem, cargoItemsToOffering)
  await cargoTable.updateCargoOffering(cargoItemsToOffering, ferryItem.link)
  await dbClient.transaction().execute(async trx => {
    const { error } = await cargoTable.updateCargoFailedOrQueuedOnTrx(ferryItem.link, failedCargoItems, trx)
    t.falsy(error)
  })

  const queuedItems = await cargoTable.selectByState(CARGO_STATE.QUEUED)
  const offeringItems = await cargoTable.selectByState(CARGO_STATE.OFFERING)
  const succeedItems = await cargoTable.selectByState(CARGO_STATE.SUCCEED)
  const failedItems = await cargoTable.selectByState(CARGO_STATE.FAILED)
  t.is(queuedItems.ok?.length, cargoItems.length - failedCargoItems.length)
  t.is(offeringItems.ok?.length, 0)
  t.is(succeedItems.ok?.length, 0)
  t.is(failedItems.ok?.length, failedCargoItems.length)

  // Validate code in failed item
  for (const f of failedCargoItems) {
    const item = failedItems.ok?.find(fc => fc.link.equals(f.link))
    t.is(item?.ferryFailedCode, f.code)
  }
})
