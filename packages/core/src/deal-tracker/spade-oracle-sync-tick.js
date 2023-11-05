import * as fzstd from 'fzstd'
import { encode, decode } from '@ipld/dag-json'
import { RecordNotFoundErrorName } from '@web3-storage/filecoin-api/errors'
import { parse as parseLink } from 'multiformats/link'
import { Piece } from '@web3-storage/data-segment'
import { toString } from 'uint8arrays/to-string'
import pAll from 'p-all'

/**
 * @typedef {import('@web3-storage/filecoin-api/deal-tracker/api').DealStore} DealStore
 * @typedef {import('./types').PieceContracts} PieceContracts
 * @typedef {import('./types').DealArchive} DealArchive
 * @typedef {import('../store/types').DealArchiveStore} DealArchiveStore
 */

/**
 * On CRON tick, this function syncs deal store entries with the most up to date information stored
 * in Spade's Oracle:
 * - The current oracle state known is fetched, as well as the latest state from Spade endpoint.
 * - Once both states are in memory, they are compared and a diff is generated.
 * - Diff is stored in deal store
 * - Handled new state of oracle is stored for comparison in next tick.
 * 
 * @param {object} context
 * @param {DealStore} context.dealStore
 * @param {DealArchiveStore} context.dealArchiveStore
 * @param {URL} context.spadeOracleUrl
 */
export async function spadeOracleSyncTick ({
  dealStore,
  dealArchiveStore,
  spadeOracleUrl
}) {
  // Get latest deal archive
  // TODO: consider doing a HEAD request and see if an ETAG is the same before proceeding
  // https://github.com/web3-storage/w3filecoin/issues/62
  const fetchLatestDealArchiveRes = await fetchLatestDealArchive(spadeOracleUrl)
  if (fetchLatestDealArchiveRes.error) {
    return fetchLatestDealArchiveRes
  }

  // Get current recorded spade oracle contracts
  const getCurrentDealArchiveRes = await getCurrentDealArchive({
    dealArchiveStore,
    spadeOracleId: spadeOracleUrl.toString(),
  })
  if (getCurrentDealArchiveRes.error && getCurrentDealArchiveRes.error.name !== RecordNotFoundErrorName) {
    return getCurrentDealArchiveRes
  }

  // Get diff of contracts
  /** @type {PieceContracts} */
  let diffPieceContracts
  if (!getCurrentDealArchiveRes.ok) {
    diffPieceContracts = fetchLatestDealArchiveRes.ok
  } else {
    diffPieceContracts = computeDiff({
      currentPieceContracts: getCurrentDealArchiveRes.ok,
      updatedPieceContracts: fetchLatestDealArchiveRes.ok
    })
  }

  // shortcut if there is no difference
  if (!diffPieceContracts.size) {
    return {
      ok: {},
      error: undefined
    }
  }

  // Store diff of contracts
  const putDiff = await putDiffToDealStore({
    dealStore,
    diffPieceContracts
  })
  if (putDiff.error) {
    return putDiff
  }

  // Record spade oracle contracts handled
  const putUpdatedSpadeOracle = await putLatestDealArchive({
    dealArchiveStore,
    spadeOracleId: spadeOracleUrl.toString(),
    oracleContracts: fetchLatestDealArchiveRes.ok
  })
  if (putUpdatedSpadeOracle.error) {
    return putUpdatedSpadeOracle
  }
  
  return {
    ok: {},
    error: undefined
  }
}

/**
 * @param {object} context
 * @param {DealStore} context.dealStore
 * @param {PieceContracts} context.diffPieceContracts
 * @returns {Promise<import('../types').Result<{}, import('@web3-storage/filecoin-api/types').StorePutError>>}
 */
export async function putDiffToDealStore ({ dealStore, diffPieceContracts }) {
  const tasks = Array.from(diffPieceContracts, ([pieceCidStr, contracts]) => {
    return () => Promise.all(contracts.map(contract => {
      /** @type {import('@web3-storage/data-segment').LegacyPieceLink} */
      const legacyPieceCid = parseLink(pieceCidStr)
      const insertedAt = new Date().toISOString()
      return dealStore.put({
        ...contract,
        // @ts-expect-error not PieceCIDv2
        piece: legacyPieceCid,
        provider: `${contract.provider}`,
        insertedAt,
        updatedAt: insertedAt
      })
    }))
  })

  const res = await pAll(tasks, { concurrency: 3 })
  const firsPutError = res.find(pieceContracts => pieceContracts.find(c => c.error))?.find(comb => comb.error)
  if (firsPutError?.error) {
    return {
      error: firsPutError.error
    }
  }
  return {
    ok: {}
  }
}

/**
 * @param {object} context
 * @param {PieceContracts} context.currentPieceContracts 
 * @param {PieceContracts} context.updatedPieceContracts 
 */
export function computeDiff ({ currentPieceContracts, updatedPieceContracts }) {
  /** @type {PieceContracts} */
  const diff = new Map()

  for (const [pieceCid, contracts] of updatedPieceContracts.entries() ) {
    const currentContracts = currentPieceContracts.get(pieceCid) || []
    const diffContracts = []
    // Get contracts for PieceCID still not recorded
    for (const c of contracts) {
      if (!currentContracts.find(pc => pc.dealId === c.dealId)) {
        diffContracts.push(c)
      }
    }
    if (diffContracts.length) {
      diff.set(pieceCid, diffContracts)
    }
  }

  return diff
}

/**
 * @param {object} context
 * @param {DealArchiveStore} context.dealArchiveStore
 * @param {string} context.spadeOracleId
 * @returns {Promise<import('../types').Result<PieceContracts, Error>>}
 */
export async function getCurrentDealArchive ({ dealArchiveStore, spadeOracleId }) {
  const getRes = await dealArchiveStore.get(spadeOracleId)
  if (getRes.error) {
    return getRes
  }

  return {
    ok: new Map(Object.entries(decode(getRes.ok.value))),
  }
}

/**
 * @param {object} context
 * @param {DealArchiveStore} context.dealArchiveStore
 * @param {string} context.spadeOracleId
 * @param {PieceContracts} context.oracleContracts 
 */
async function putLatestDealArchive ({ dealArchiveStore, spadeOracleId, oracleContracts }) {
  const putRes = await dealArchiveStore.put({
    key: spadeOracleId,
    value: encode(Object.fromEntries(oracleContracts))
  })

  return putRes
}

/**
 * @param {URL} spadeOracleUrl
 * @returns {Promise<import('../types').Result<PieceContracts, Error>>}
 */
async function fetchLatestDealArchive (spadeOracleUrl) {
  /** @type {PieceContracts} */
  const dealMap = new Map()
  const res = await fetch(spadeOracleUrl)
  if (!res.ok) {
    return {
      error: new Error(`unexpected response status fetching deal archive: ${res.status}`)
    }
  }

  const compressed = new Uint8Array(await res.arrayBuffer())
  const decompressed = fzstd.decompress(compressed)
  const resDecompressed = toString(decompressed)

  /** @type {DealArchive} */
  const dealArchive = JSON.parse(resDecompressed)
  for (const replica of dealArchive.active_replicas) {
    // Convert PieceCidV1 to PieceCidV2
    const pieceCid = convertPieceCidV1toPieceCidV2(
      parseLink(replica.piece_cid),
      replica.piece_log2_size
    )
    dealMap.set(pieceCid.toString(), replica.contracts.map(c => ({
      provider: c.provider_id,
      dealId: c.legacy_market_id,
      expirationEpoch: c.legacy_market_end_epoch,
      source: spadeOracleUrl.toString()
    })))
  }
  
  return {
    ok: dealMap
  }
}

/**
 * @param {import('@web3-storage/data-segment').LegacyPieceLink} link
 * @param {number} height
 */
export function convertPieceCidV1toPieceCidV2 (link, height) {
  const piece = Piece.toView({
    root: link.multihash.digest,
    height,
    // Aggregates do not have padding
    padding: 0n
  })

  return piece.link
}
