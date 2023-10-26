import { testDealTracker as test } from '../helpers/context.js'
import { createS3, createBucket, createDynamodDb, createTable } from '../helpers/resources.js'

import { parse as parseLink } from 'multiformats/link'
import { encode, decode } from '@ipld/dag-json'

import { dealStoreTableProps } from '../../src/store/index.js'
import { createClient as createDealStoreClient } from '../../src/store/deal-store.js'
import { createClient as createSpadeOracleStoreClient } from '../../src/store/spade-oracle-store.js'
import * as spadeOracleSyncTick from '../../src/deal-tracker/spade-oracle-sync-tick.js'
import { RecordNotFoundErrorName } from '@web3-storage/filecoin-api/errors'

test.beforeEach(async (t) => {
  const dynamo = await createDynamodDb()
  Object.assign(t.context, {
    s3: (await createS3()).client,
    dynamoClient: dynamo.client,
  })
})

test('downloads spade oracle replicas file from http server', async t => {
  const {
    dealStore,
    spadeOracleStore,
    spadeOracleUrl
  } = await getContext(t.context)

  const spadeOracleSyncTickHandle = await spadeOracleSyncTick.spadeOracleSyncTick({
    dealStore,
    spadeOracleStore,
    spadeOracleUrl
  })
  t.truthy(spadeOracleSyncTickHandle.ok)
  t.falsy(spadeOracleSyncTickHandle.error)

  // Verify spade oracle was stored for future check
  const getSpadeOracle = await spadeOracleStore.get(spadeOracleUrl.toString())
  if (getSpadeOracle.error) { 
    throw new Error('could get get spade oracle stored')
  }
  t.truthy(getSpadeOracle.ok)

  /** @type {import('../../src/deal-tracker/types.js').OracleContracts} */
  const oracleState = new Map(Object.entries(decode(getSpadeOracle.ok.value)))
  t.is(oracleState.size, 2)

  // Verify deals were stored
  for (const [pieceCid, contracts] of oracleState.entries()) {
    t.is(contracts.length, 2)
    for (const c of contracts) {
      const piece = parseLink(pieceCid)
      const getDealEntry = await dealStore.get({
        // @ts-expect-error old piece CID
        piece,
        dealId: c.dealId
      })
      if (getDealEntry.error) {
        throw new Error('could get get deal entry stored')
      }

      t.truthy(getDealEntry.ok)
      t.truthy(piece.equals(getDealEntry.ok.piece))
      t.is(c.source, getDealEntry.ok.source)
      t.is(c.dealId, getDealEntry.ok.dealId)
      t.is(String(c.provider), getDealEntry.ok.provider)
      t.is(c.expirationEpoch, getDealEntry.ok.expirationEpoch)
      t.truthy(getDealEntry.ok.insertedAt)
    }
  }
})

test('gets previous spade oracle state if available', async t => {
  const {
    spadeOracleStore,
    spadeOracleUrl
  } = await getContext(t.context)
  const spadeOracleId = spadeOracleUrl.toString()
  const source = encodeURIComponent(spadeOracleUrl.toString())
  const oracleContracts = getOracleContracts(source)

  // Try to get init state
  const getSpadeOracleStateInit = await spadeOracleSyncTick.getSpadeOracleState({
    spadeOracleStore,
    spadeOracleId,
  })
  t.falsy(getSpadeOracleStateInit.ok)
  t.truthy(getSpadeOracleStateInit.error)
  t.is(getSpadeOracleStateInit.error?.name, RecordNotFoundErrorName)

  // Populate store with first diff
  const putSpadeOracleState = await spadeOracleStore.put({
    key: spadeOracleId,
    value: encode(Object.fromEntries(oracleContracts))
  })
  t.truthy(putSpadeOracleState.ok)

  // Get state and validate
  const getSpadeOracleState = await spadeOracleSyncTick.getSpadeOracleState({
    spadeOracleStore,
    spadeOracleId,
  })
  if (getSpadeOracleState.error) { 
    throw new Error('could get get spade oracle stored')
  }
  t.truthy(getSpadeOracleState.ok)
  t.is(getSpadeOracleState.ok.size, oracleContracts.size)
  for (const [pieceCid, contracts] of getSpadeOracleState.ok.entries()) {
    const insertedContracts = oracleContracts.get(pieceCid)
    t.truthy(insertedContracts)
    t.deepEqual(insertedContracts, contracts)
  }
})

test('computes diff', async t => {
  const {
    dealStore,
    spadeOracleUrl
  } = await getContext(t.context)

  // Get previous oracle contracts
  const source = encodeURIComponent(spadeOracleUrl.toString())
  const previousOracleContracts = getOracleContracts(source)

  // Store diff as previous oracle contracts
  const putDiffInit = await spadeOracleSyncTick.putDiffToDealStore({
    dealStore,
    diffOracleContracts: previousOracleContracts
  })
  t.truthy(putDiffInit.ok)

  // Verify pieces before updates
  const alreadyExistingPieceCid = 'baga6ea4seaqhmw7z7q3jypdr54xaluhzdn6syn7ovovvjpaqul2qqenhmg43wii'
  const newPieceCid = 'baga6ea4seaqlskmw3rlwyebtplguyvr7rmuofydmnud2o6a5soyydgcede56kkq'
  const queryInitAlreadyExisting = await dealStore.query({
    piece: parseLink(alreadyExistingPieceCid)
  })
  t.truthy(queryInitAlreadyExisting.ok)
  t.is(queryInitAlreadyExisting.ok?.length, 2)

  const queryInitNew = await dealStore.query({
    piece: parseLink(newPieceCid)
  })
  t.truthy(queryInitNew.ok)
  t.is(queryInitNew.ok?.length, 0)

  // Create updated oracle
  const updatedOracleContracts = getOracleContracts(source)
  // Add one more contract to one existing item
  const itemToAddContract = updatedOracleContracts.get(alreadyExistingPieceCid) || []
  itemToAddContract?.push({
    provider: 2095199,
    dealId: 40745773,
    expirationEpoch: 4477915,
    source
  })
  updatedOracleContracts.set(alreadyExistingPieceCid, itemToAddContract)

  // Add new piece contracts
  updatedOracleContracts.set(
    newPieceCid,
    [
      {
        provider: 20378,
        dealId: 41028500,
        expirationEpoch: 4482396,
        source
      }
    ]
  )

  const diffOracleContracts = spadeOracleSyncTick.computeDiffOracleState({
    previousOracleContracts,
    updatedOracleContracts
  })
  t.is(diffOracleContracts.size, 2)
  // 1 new item in each to update
  t.truthy(diffOracleContracts.get(alreadyExistingPieceCid)?.length === 1)
  t.truthy(diffOracleContracts.get(newPieceCid)?.length === 1)

  // Store diff
  const putDiffResult = await spadeOracleSyncTick.putDiffToDealStore({
    dealStore,
    diffOracleContracts
  })
  t.truthy(putDiffResult.ok)

  const queryAfterDiffAlreadyExisting = await dealStore.query({
    piece: parseLink(alreadyExistingPieceCid)
  })
  t.truthy(queryAfterDiffAlreadyExisting.ok)
  t.is(queryAfterDiffAlreadyExisting.ok?.length, 3)

  const queryAfterDiffNew = await dealStore.query({
    piece: parseLink(newPieceCid)
  })
  t.truthy(queryAfterDiffNew.ok)
  t.is(queryAfterDiffNew.ok?.length, 1)
})

/**
 * @param {string} source 
 */
function getOracleContracts (source) {
  /** @type {import('../../src/deal-tracker/types.js').OracleContracts} */
  const oracleContracts = new Map()
  oracleContracts.set(
    'baga6ea4seaqhmw7z7q3jypdr54xaluhzdn6syn7ovovvjpaqul2qqenhmg43wii',
    [
      {
        provider: 2095132,
        dealId: 40745772,
        expirationEpoch: 4477915,
        source
      },
      {
        provider: 20378,
        dealId: 41028577,
        expirationEpoch: 4482396,
        source
      }
    ]
  )
  oracleContracts.set(
    'baga6ea4seaqgmg7ogugsyopv4gkbrgugiaq5mn6kofngpbot4gulgllv5q3kmei',
    [
      {
        provider: 2095132,
        dealId: 38117821,
        expirationEpoch: 4429240,
        source
      },
      {
        provider: 1784458,
        dealId: 46363131,
        expirationEpoch: 4580799,
        source
      }
    ]
  )

  return oracleContracts
}

/**
 * @param {import('../helpers/context.js').BucketContext & import('../helpers/context.js').DbContext} context
 */
async function getContext (context) {
  const { s3, dynamoClient } = context
  const bucketName = await createBucket(s3)
  const tableName = await createTable(dynamoClient, dealStoreTableProps)

  const dealStore = createDealStoreClient(dynamoClient, {
    tableName
  })
  const spadeOracleStore = createSpadeOracleStoreClient(s3, {
    name: bucketName
  })

  return {
    dealStore,
    spadeOracleStore,
    spadeOracleUrl: new URL(`http://127.0.0.1:${process.env.PORT || 9000}`)
  }
}
