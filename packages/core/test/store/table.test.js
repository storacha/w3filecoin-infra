import { testStore as test } from '../helpers/context.js'
import {
  createTable,
  createDynamodDb,
} from '../helpers/resources.js'
import { randomCargo, randomAggregate } from '../helpers/cargo.js'

import { encode as pieceEncode, decode as pieceDecode } from '../../src/data/piece.js'
import { encode as aggregateEncode, decode as aggregateDecode } from '../../src/data/aggregate.js'
import { encode as inclusionEncode, decode as inclusionDecode } from '../../src/data/inclusion.js'
import { createTableStoreClient } from '../../src/store/table-client.js'
import { pieceStoreTableProps, aggregateStoreTableProps, inclusionStoreTableProps } from '../../src/store/index.js'

test.before(async (t) => {
  Object.assign(t.context, {
    dynamoClient: (await createDynamodDb()).client,
  })
})

test('can put and get piece record from piece store', async t => {
  const { dynamoClient } = t.context
  const tableName = await createTable(dynamoClient, pieceStoreTableProps)
  const [cargo] = await randomCargo(1, 128)

  const pieceStore = createTableStoreClient(dynamoClient, {
    tableName,
    encodeRecord: pieceEncode.storeRecord,
    decodeRecord: pieceDecode.storeRecord,
    encodeKey: pieceEncode.storeKey
  })
  t.truthy(pieceStore)

  const piece = cargo.link
  const storefront = 'did:web:web3.storage'
  const group = 'did:web:free.web3.storage'
  const pieceRow = {
    piece,
    storefront,
    group,
    insertedAt: Date.now()
  }

  const putRes = await pieceStore.put(pieceRow)
  t.truthy(putRes.ok)
  t.falsy(putRes.error)

  const getRes = await pieceStore.get({
    piece,
    storefront
  })
  t.truthy(getRes.ok)
  t.falsy(getRes.error)
  t.deepEqual(pieceRow, getRes.ok)
})

test('can put and get aggregate record from aggregate store', async t => {
  const { dynamoClient } = t.context
  const tableName = await createTable(dynamoClient, aggregateStoreTableProps)
  const { aggregate, pieces } = await randomAggregate(10, 128)
  const storefront = 'did:web:web3.storage'
  const group = 'did:web:free.web3.storage'

  const aggregateStore = createTableStoreClient(dynamoClient, {
    tableName,
    encodeRecord: aggregateEncode.storeRecord,
    decodeRecord: aggregateDecode.storeRecord,
    encodeKey: aggregateEncode.storeKey
  })
  t.truthy(aggregateStore)

  const aggregateRow = {
    piece: aggregate.link,
    buffer: pieces[0].content, // random CID for testing
    insertedAt: Date.now(),
    storefront,
    group,
    stat: /** @type {import('../../src/data/types.js').AggregateStatus} */ (0),
  }
  const putRes = await aggregateStore.put(aggregateRow)
  t.truthy(putRes.ok)
  t.falsy(putRes.error)

  const getRes = await aggregateStore.get({
    piece: aggregate.link
  })
  t.truthy(getRes.ok)
  t.falsy(getRes.error)
  t.deepEqual({
    ...aggregateRow,
    invocation: undefined,
    task: undefined
  }, getRes.ok)
})

test('can put and get inclusion record from inclusion store', async t => {
  const { dynamoClient } = t.context
  const tableName = await createTable(dynamoClient, inclusionStoreTableProps)
  const { aggregate, pieces } = await randomAggregate(10, 128)

  const inclusionStore = createTableStoreClient(dynamoClient, {
    tableName,
    encodeRecord: inclusionEncode.storeRecord,
    decodeRecord: inclusionDecode.storeRecord,
    encodeKey: inclusionEncode.storeKey
  })

  t.truthy(inclusionStore)

  const inclusionRow = {
    aggregate: aggregate.link,
    piece: pieces[0].link,
    insertedAt: Date.now(),
    submitedAt: Date.now(),
    resolvedAt: Date.now(),
    stat: /** @type {import('../../src/data/types.js').InclusionStatus} */ (0),
  }
  const putRes = await inclusionStore.put(inclusionRow)
  t.truthy(putRes.ok)
  t.falsy(putRes.error)

  const getRes = await inclusionStore.get({
    aggregate: aggregate.link,
    piece: pieces[0].link,
  })
  t.truthy(getRes.ok)
  t.falsy(getRes.error)
  t.deepEqual({
    ...inclusionRow,
    failedReason: undefined
  }, getRes.ok)
})
