import { test as filecoinApiTest } from '@storacha/filecoin-api/test'
import { ed25519 } from '@ucanto/principal'
import delay from 'delay'

import { createClient } from '../src/store/deal-store.js'
import { dealStoreTableProps } from '../src/store/index.js'

import { testService as test } from './helpers/context.js'
import { createDynamodDb, createTable } from './helpers/resources.js'

test.before(async (t) => {
  const { client: dynamoClient, stop: dynamoStop } = await createDynamodDb()

  Object.assign(t.context, {
    dynamoClient,
    stop: async () => {
      await dynamoStop()
    }
  })
})

test.after(async (t) => {
  await t.context.stop()
  await delay(1000)
})

for (const [title, unit] of Object.entries(
  filecoinApiTest.service.dealTracker
)) {
  const define = title.startsWith('only ')
    ? // eslint-disable-next-line no-only-tests/no-only-tests
    test.only
    : title.startsWith('skip ')
      ? test.skip
      : test

  define(title, async (t) => {
    const { dynamoClient } = t.context
    const tableName = await createTable(dynamoClient, dealStoreTableProps)

    // context
    const signer = await ed25519.generate()
    const id = signer.withDID('did:web:test.web3.storage')
    const dealStore = createClient(dynamoClient, {
      tableName
    })

    await unit(
      {
        ok: (actual, message) => t.truthy(actual, message),
        equal: (actual, expect, message) =>
          t.is(actual, expect, message ? String(message) : undefined),
        deepEqual: (actual, expect, message) =>
          t.deepEqual(actual, expect, message ? String(message) : undefined)
      },
      {
        id,
        errorReporter: {
          catch (error) {
            t.fail(error.message)
          }
        },
        dealStore,
        queuedMessages: new Map(),
        validateAuthorization: () => ({ ok: {} })
      }
    )
  })
}
