import { createClient as createAggregateStoreClient } from '@w3filecoin/core/src/store/dealer-aggregate-store.js'

const AWS_REGION = 'us-west-2'

const aggregateStore = createAggregateStoreClient({
  region: AWS_REGION
}, {
  tableName: 'prod-w3filecoin-dealer-aggregate-store'
})

// Get offered aggregates pending approval/rejection
const offeredAggregates = await aggregateStore.query({
  status: 'offered',
})
if (offeredAggregates.error) {
  throw offeredAggregates.error
}

console.log('Offered aggregates page size:', offeredAggregates.ok.length, '\n')
console.log('Aggregate offer list:')
for (const aggregate of offeredAggregates.ok) {
  console.log(`${aggregate.aggregate.link()} at ${aggregate.insertedAt}`)
}
