import { test as filecoinApiTest } from '@storacha/filecoin-api/test'
import * as Signer from '@ucanto/principal/ed25519'
import delay from 'delay'

import { createClient as createAggregateStoreClient } from '@w3filecoin/core/src/store/dealer-aggregate-store.js'
import { createClient as createOfferStoreClient } from '@w3filecoin/core/src/store/dealer-offer-store.js'
import { dealerAggregateStoreTableProps } from '@w3filecoin/core/src/store/index.js'

import { testStore as test } from '@w3filecoin/core/test/helpers/context.js'
import {
  getMockService,
  getConnection
} from '@storacha/filecoin-api/test/context/service'
import {
  createDynamodDb,
  createTable,
  createS3,
  createBucket
} from '@w3filecoin/core/test/helpers/resources.js'

test.before(async (t) => {
  const { client: s3Client, stop: s3Stop } = await createS3({ port: 9000 })
  const { client: dynamoClient, stop: dynamoStop } = await createDynamodDb()

  Object.assign(t.context, {
    s3: s3Client,
    dynamoClient,
    stop: async () => {
      await s3Stop()
      await dynamoStop()
    }
  })
})

test.after(async (t) => {
  await t.context.stop()
  await delay(1000)
})

for (const [title, unit] of Object.entries(filecoinApiTest.events.dealer)) {
  const define = title.startsWith('only ')
    ? // eslint-disable-next-line no-only-tests/no-only-tests
    test.only
    : title.startsWith('skip ')
      ? test.skip
      : test

  define(title, async (t) => {
    const { dynamoClient, s3 } = t.context
    const bucketName = await createBucket(s3)
    const tableName = await createTable(
      dynamoClient,
      dealerAggregateStoreTableProps
    )

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
    const dealerConnection = getConnection(dealerSigner, service).connection
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
          t.deepEqual(actual, expect, message ? String(message) : undefined)
      },
      {
        id: dealerSigner,
        aggregateStore,
        errorReporter: {
          catch (error) {
            t.fail(error.message)
          }
        },
        queuedMessages: new Map(),
        offerStore,
        dealerService: {
          connection: dealerConnection,
          invocationConfig: {
            issuer: dealerSigner,
            with: dealerSigner.did(),
            audience: dealerSigner
          }
        },
        dealTrackerService: {
          connection: dealTrackerConnection,
          invocationConfig: {
            issuer: dealerSigner,
            with: dealerSigner.did(),
            audience: dealTrackerSigner
          }
        },
        service,
        validateAuthorization: () => ({ ok: {} })
      }
    )
  })
}
