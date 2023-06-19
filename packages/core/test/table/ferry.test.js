import { test } from '../helpers/context.js'

import { useCargoTable, STATE as CARGO_STATE } from '../../src/table/cargo.js'
import { useFerryTable, STATE as FERRY_STATE } from '../../src/table/ferry.js'

import { createDatabase } from '../helpers/resources.js'
import { getCargo } from '../helpers/cargo.js'

test.beforeEach(async (t) => {
  Object.assign(t.context, {
    dbClient: (await createDatabase()).client,
  })
})

test('can insert ferry table and add cargo to it', async t => {
  const { dbClient } = t.context
  const cargoTable = useCargoTable(dbClient)
  const ferryTable = useFerryTable(dbClient)
  const cargoItems = await getCargo(10)

  await Promise.all(
    cargoItems.map(cargo => cargoTable.insert(cargo))
  )

  // TODO: get proper aggregate link and size
  const ferryItem = {
    link: cargoItems[0].link,
    size: cargoItems.reduce((accum, item) => item.size + accum, 0)
  }
  await ferryTable.insert(ferryItem, cargoItems.map(i => i.link))

  const ferryItems = await ferryTable.selectByState(FERRY_STATE.QUEUED)
  if (!ferryItems.ok) {
    throw new Error('could not get ferry items')
  }
  t.is(ferryItems.ok.length, 1)
  t.assert(ferryItems.ok[0])
  t.truthy(ferryItem.link.equals(ferryItems.ok[0].link))
  t.is(ferryItem.size, ferryItems.ok[0].size)
  t.is(ferryItems.ok[0].state, FERRY_STATE.QUEUED)

  const queuedCargoItems = await cargoTable.selectByState(CARGO_STATE.QUEUED)
  t.is(queuedCargoItems.ok?.length, 0)

  const offeringCargoItems = await cargoTable.selectByState(CARGO_STATE.OFFERING)
  t.is(offeringCargoItems.ok?.length, cargoItems.length)
})

test('update ferry to arranging offer', async t => {
  const { dbClient } = t.context
  const cargoTable = useCargoTable(dbClient)
  const ferryTable = useFerryTable(dbClient)

  const cargoItems = await getCargo(10)

  await Promise.all(
    cargoItems.map(cargo => cargoTable.insert(cargo))
  )

  // TODO: get proper aggregate link and size
  const ferryItem = {
    link: cargoItems[0].link,
    size: cargoItems.reduce((accum, item) => item.size + accum, 0)
  }
  await ferryTable.insert(ferryItem, cargoItems.map(i => i.link))

  const { error } = await ferryTable.updateFerryToArranging(ferryItem.link)
  t.falsy(error)

  const queuedFerries = await ferryTable.selectByState(FERRY_STATE.QUEUED)
  const arrangingFerries = await ferryTable.selectByState(FERRY_STATE.ARRANGING)
  t.is(queuedFerries.ok?.length, 0)
  t.is(arrangingFerries.ok?.length, 1)
})

test('fails to update ferry to arranging offer when already in state', async t => {
  const { dbClient } = t.context
  const cargoTable = useCargoTable(dbClient)
  const ferryTable = useFerryTable(dbClient)

  const cargoItems = await getCargo(10)

  await Promise.all(
    cargoItems.map(cargo => cargoTable.insert(cargo))
  )

  // TODO: get proper aggregate link and size
  const ferryItem = {
    link: cargoItems[0].link,
    size: cargoItems.reduce((accum, item) => item.size + accum, 0)
  }
  await ferryTable.insert(ferryItem, cargoItems.map(i => i.link))
  await ferryTable.updateFerryToArranging(ferryItem.link)

  const { error: errorNotInQueued } = await ferryTable.updateFerryToArranging(ferryItem.link)
  t.truthy(errorNotInQueued)
})

test('update ferry to succeed offer', async t => {
  const { dbClient } = t.context
  const cargoTable = useCargoTable(dbClient)
  const ferryTable = useFerryTable(dbClient)

  const cargoItems = await getCargo(10)

  await Promise.all(
    cargoItems.map(cargo => cargoTable.insert(cargo))
  )

  // TODO: get proper aggregate link and size
  const ferryItem = {
    link: cargoItems[0].link,
    size: cargoItems.reduce((accum, item) => item.size + accum, 0)
  }
  await ferryTable.insert(ferryItem, cargoItems.map(i => i.link))
  await ferryTable.updateFerryToArranging(ferryItem.link)
  const { error } = await ferryTable.updateFerryToSucceed(ferryItem.link)
  t.falsy(error)

  const queuedFerries = await ferryTable.selectByState(FERRY_STATE.QUEUED)
  const arrangingFerries = await ferryTable.selectByState(FERRY_STATE.ARRANGING)
  const succeedFerries = await ferryTable.selectByState(FERRY_STATE.SUCCEED)
  const failedFerries = await ferryTable.selectByState(FERRY_STATE.FAILED)
  t.is(queuedFerries.ok?.length, 0)
  t.is(arrangingFerries.ok?.length, 0)
  t.is(succeedFerries.ok?.length, 1)
  t.is(failedFerries.ok?.length, 0)

  // Validate cargo as succeed
  const succeedCargo = await cargoTable.selectByState(CARGO_STATE.SUCCEED)
  t.is(succeedCargo.ok?.length, cargoItems.length)
})

test('fails to update ferry to succeed if not in arranging in state', async t => {
  const { dbClient } = t.context
  const cargoTable = useCargoTable(dbClient)
  const ferryTable = useFerryTable(dbClient)

  const cargoItems = await getCargo(10)

  await Promise.all(
    cargoItems.map(cargo => cargoTable.insert(cargo))
  )

  // TODO: get proper aggregate link and size
  const ferryItem = {
    link: cargoItems[0].link,
    size: cargoItems.reduce((accum, item) => item.size + accum, 0)
  }
  await ferryTable.insert(ferryItem, cargoItems.map(i => i.link))

  // Try to succeed beforehand
  const { error: errorNotArranging } = await ferryTable.updateFerryToSucceed(ferryItem.link)
  t.truthy(errorNotArranging)

  await ferryTable.updateFerryToArranging(ferryItem.link)
  await ferryTable.updateFerryToSucceed(ferryItem.link)

  // Try to succeed afterwards
  const { error: errorAlreadySucceed } = await ferryTable.updateFerryToSucceed(ferryItem.link)
  t.truthy(errorAlreadySucceed)
})

test('update ferry to failed offer', async t => {
  const { dbClient } = t.context
  const cargoTable = useCargoTable(dbClient)
  const ferryTable = useFerryTable(dbClient)

  const cargoItems = await getCargo(10)

  await Promise.all(
    cargoItems.map(cargo => cargoTable.insert(cargo))
  )

  // TODO: get proper aggregate link and size
  const ferryItem = {
    link: cargoItems[0].link,
    size: cargoItems.reduce((accum, item) => item.size + accum, 0)
  }
  await ferryTable.insert(ferryItem, cargoItems.map(i => i.link))
  await ferryTable.updateFerryToArranging(ferryItem.link)

  const failedCargoItems = [{
    link: cargoItems[0].link,
    code: 'INVALID CARGO'
  }]
  const { error } = await ferryTable.updateFerryToFailed(ferryItem.link, failedCargoItems)
  t.falsy(error)

  const queuedFerries = await ferryTable.selectByState(FERRY_STATE.QUEUED)
  const arrangingFerries = await ferryTable.selectByState(FERRY_STATE.ARRANGING)
  const succeedFerries = await ferryTable.selectByState(FERRY_STATE.SUCCEED)
  const failedFerries = await ferryTable.selectByState(FERRY_STATE.FAILED)
  t.is(queuedFerries.ok?.length, 0)
  t.is(arrangingFerries.ok?.length, 0)
  t.is(succeedFerries.ok?.length, 0)
  t.is(failedFerries.ok?.length, 1)

  // Validate cargo as failed
  const failedCargo = await cargoTable.selectByState(CARGO_STATE.FAILED)
  t.is(failedCargo.ok?.length, failedCargoItems.length)
  
  // Validate code in failed item
  for (const f of failedCargoItems) {
    const item = failedCargo.ok?.find(fc => fc.link.equals(f.link))
    t.is(item?.ferryFailedCode, f.code)
  }

  // Validate cargo as queued
  const queuedCargo = await cargoTable.selectByState(CARGO_STATE.QUEUED)
  t.is(queuedCargo.ok?.length, cargoItems.length - failedCargoItems.length)
})

test('fails to update ferry to failed if not in arranging in state', async t => {
  const { dbClient } = t.context
  const cargoTable = useCargoTable(dbClient)
  const ferryTable = useFerryTable(dbClient)

  const cargoItems = await getCargo(10)

  await Promise.all(
    cargoItems.map(cargo => cargoTable.insert(cargo))
  )

  // TODO: get proper aggregate link and size
  const ferryItem = {
    link: cargoItems[0].link,
    size: cargoItems.reduce((accum, item) => item.size + accum, 0)
  }
  await ferryTable.insert(ferryItem, cargoItems.map(i => i.link))

  const failedCargoItems = [{
    link: cargoItems[0].link,
    code: 'INVALID CARGO'
  }]
  const { error: errorNotArranging } = await ferryTable.updateFerryToFailed(ferryItem.link, failedCargoItems)
  t.truthy(errorNotArranging)

  await ferryTable.updateFerryToArranging(ferryItem.link)
  await ferryTable.updateFerryToFailed(ferryItem.link, failedCargoItems)

  // Try to fail afterwards
  const { error: errorAlreadyFailed } = await ferryTable.updateFerryToFailed(ferryItem.link, failedCargoItems)
  t.truthy(errorAlreadyFailed)
})
