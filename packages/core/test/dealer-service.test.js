import { test as filecoinApiTest } from '@web3-storage/filecoin-api/test'
import { ed25519 } from '@ucanto/principal'

import { createClient as createAggregateStoreClient } from '../src/store/dealer-aggregate-store.js'
import { createClient as createOfferStoreClient } from '../src/store/dealer-offer-store.js'
import { dealerAggregateStoreTableProps } from '../src/store/index.js'

import { testStore as test } from './helpers/context.js'
import { createDynamodDb, createTable, createS3, createBucket } from './helpers/resources.js'

test.beforeEach(async (t) => {
  const dynamo = await createDynamodDb()

  Object.assign(t.context, {
    s3: (await createS3()).client,
    dynamoClient: dynamo.client,
  })
})

for (const [title, unit] of Object.entries(filecoinApiTest.service.dealer)) {
  const define = title.startsWith('only ')
    // eslint-disable-next-line no-only-tests/no-only-tests
    ? test.only
    : title.startsWith('skip ')
    ? test.skip
    : test

  define(title, async (t) => {
    const { dynamoClient, s3 } = t.context
    const bucketName = await createBucket(s3)
    const tableName = await createTable(dynamoClient, dealerAggregateStoreTableProps)

    // context
    const signer = await ed25519.generate()
    const id = signer.withDID('did:web:test.web3.storage')
    const aggregateStore = createAggregateStoreClient(dynamoClient, {
      tableName
    })
    const offerStore = createOfferStoreClient(s3, {
      name: bucketName
    })

    await unit(
      {
        ok: (actual, message) => t.truthy(actual, message),
        equal: (actual, expect, message) =>
          t.is(actual, expect, message ? String(message) : undefined),
        deepEqual: (actual, expect, message) =>
          t.deepEqual(actual, expect, message ? String(message) : undefined),
      },
      {
        id,
        aggregateStore,
        offerStore,
        errorReporter: {
          catch(error) {
            t.fail(error.message)
          },
        },
        queuedMessages: new Map(),
        validateAuthorization: () => ({ ok: {} })
      }
    )
  })
}
