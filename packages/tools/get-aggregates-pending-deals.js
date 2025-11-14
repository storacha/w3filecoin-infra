import { createClient as createAggregateStoreClient } from '@w3filecoin/core/src/store/dealer-aggregate-store.js'

const AWS_REGION = 'us-west-2'

const aggregateStore = createAggregateStoreClient({
  region: AWS_REGION
}, {
  tableName: 'prod-w3filecoin-dealer-aggregate-store'
})

console.log('Aggregate offer list:')
/** @type {string|undefined} */
let cursor
do {
  // Get offered aggregates pending approval/rejection
  const offeredAggregates = await aggregateStore.query({
    status: 'offered'
  }, { cursor })
  if (offeredAggregates.error) {
    throw offeredAggregates.error
  }

  for (const aggregate of offeredAggregates.ok.results) {
    console.log(`${aggregate.aggregate.link()} at ${aggregate.insertedAt}`)
  }
  cursor = offeredAggregates.ok.cursor
} while (cursor)
