import { test as filecoinApiTest } from '@web3-storage/filecoin-api/test'
import * as Signer from '@ucanto/principal/ed25519'
import delay from 'delay'

import { createClient as createAggregateStoreClient } from '../src/store/dealer-aggregate-store.js'
import { createClient as createOfferStoreClient } from '../src/store/dealer-offer-store.js'
import { dealerAggregateStoreTableProps } from '../src/store/index.js'

import { testStore as test } from './helpers/context.js'
import { getMockService, getConnection } from '@web3-storage/filecoin-api/test/context/service'
import { createDynamodDb, createTable, createS3, createBucket } from './helpers/resources.js'

test.before(async (t) => {
  const dynamo = await createDynamodDb()

  Object.assign(t.context, {
    s3: (await createS3()).client,
    dynamoClient: dynamo.client,
  })
})

test.after(async t => {
  await delay(1000)
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
    const dealerSigner = await Signer.generate()
    const dealTrackerSigner = await Signer.generate()
    const aggregateStore = createAggregateStoreClient(dynamoClient, {
      tableName
    })
    const offerStore = createOfferStoreClient(s3, {
      name: bucketName
    })
    const service = getMockService()
    const dealTrackerConnection = getConnection(
      dealTrackerSigner,
      service
    ).connection

    await unit(
      {
        ok: (actual, message) => t.truthy(actual, message),
        equal: (actual, expect, message) =>
          t.is(actual, expect, message ? String(message) : undefined),
        deepEqual: (actual, expect, message) =>
          t.deepEqual(actual, expect, message ? String(message) : undefined),
      },
      {
        id: dealerSigner,
        aggregateStore,
        offerStore,
        dealTrackerService: {
          connection: dealTrackerConnection,
          invocationConfig: {
            issuer: dealerSigner,
            with: dealerSigner.did(),
            audience: dealTrackerSigner,
          },
        },
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
