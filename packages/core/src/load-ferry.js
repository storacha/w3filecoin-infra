import { STATE as CARGO_STATE } from './table/cargo.js'
import { DatabaseUniqueValueConstraintErrorName } from './table/errors.js'

/**
 * 
 * @param {import('./types').CargoTable} cargoTable 
 * @param {import('./types').FerryTable} ferryTable 
 */
export async function loadFerry(cargoTable, ferryTable) {
  // TODO: do while we have cargo in loop
  // TODO: next page if not enough
  const { ok: queuedCargo, error: queuedCargoError } = await cargoTable.selectByState(
    CARGO_STATE.QUEUED, {
    limit: 1000
    }
  )

  if (queuedCargoError) {
    throw queuedCargoError
  }

  // TODO: use aggregate builder
  const ferryItem = {
    link: queuedCargo[0].link,
    size: queuedCargo[0].size,
  }
  const cargoItems = queuedCargo.map(c => c.link)
  const { error: insertFerryError } = await ferryTable.insert(ferryItem, cargoItems)

  // Ferry already changed state due to concurrent operation
  // so it is safe to just ignore error
  if(insertFerryError?.name === DatabaseUniqueValueConstraintErrorName) {
    return
  } else if (insertFerryError) {
    throw insertFerryError
  }
}
