// @ts-expect-error no types available
import { ZSTDDecompress } from 'simple-zstd'
import { Readable } from 'stream'
// @ts-expect-error no types available
import streamReadAll from 'stream-read-all'
import { toString } from 'uint8arrays/to-string'
import { encode, decode } from '@ipld/dag-json'
import { RecordNotFoundErrorName } from '@web3-storage/filecoin-api/errors'
import { parse as parseLink } from 'multiformats/link'
import { Piece } from '@web3-storage/data-segment'

/**
 * @typedef {import('@web3-storage/filecoin-api/deal-tracker/api').DealStore} DealStore
 * @typedef {import('./types').OracleContracts} OracleContracts
 * @typedef {import('./types').SpadeOracle} SpadeOracle
 * @typedef {import('../store/types').SpadeOracleStore} SpadeOracleStore
 */

/**
 * On CRON tick, this function syncs deal store entries with the most up to date information stored
 * in Spade's Oracle:
 * - The previous oracle state known is fetched, as well as the latest state from Spade endpoint.
 * - Once both states are in memory, they are compared and a diff is generated.
 * - Diff is stored in deal store
 * - Handled new state of oracle is stored for comparison in next tick.
 * 
 * @param {object} context
 * @param {DealStore} context.dealStore
 * @param {SpadeOracleStore} context.spadeOracleStore
 * @param {URL} context.spadeOracleUrl
 */
export async function spadeOracleSyncTick ({
  dealStore,
  spadeOracleStore,
  spadeOracleUrl
}) {
  // Get previous recorded spade oracle contracts
  const getPreviousSpadeOracle = await getSpadeOracleState({
    spadeOracleStore,
    spadeOracleId: spadeOracleUrl.toString(),
  })
  if (getPreviousSpadeOracle.error && getPreviousSpadeOracle.error.name !== RecordNotFoundErrorName) {
    return getPreviousSpadeOracle
  }

  // Get updated spade oracle contracts
  const getUpdatedSpadeOracle = await getSpadeOracleCurrentState(spadeOracleUrl)
  if (getUpdatedSpadeOracle.error) {
    return getUpdatedSpadeOracle
  }

  // Get diff of contracts
  const diffOracleContracts = computeDiffOracleState({
    // fallsback to empty map if not found
    previousOracleContracts: getPreviousSpadeOracle.ok || new Map(),
    updatedOracleContracts: getUpdatedSpadeOracle.ok
  })

  // Store diff of contracts
  const putDiff = await putDiffToDealStore({
    dealStore,
    diffOracleContracts
  })
  if (putDiff.error) {
    return putDiff
  }

  // Record spade oracle contracts handled
  const putUpdatedSpadeOracle = await putUpdatedSpadeOracleState({
    spadeOracleStore,
    spadeOracleId: spadeOracleUrl.toString(),
    oracleContracts: getUpdatedSpadeOracle.ok
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
 * @param {OracleContracts} context.diffOracleContracts
 * @returns {Promise<import('../types').Result<{}, import('@web3-storage/filecoin-api/types').StorePutError>>}
 */
export async function putDiffToDealStore ({ dealStore, diffOracleContracts }) {
  const res = await Promise.all(
    Array.from(diffOracleContracts, ([key, value]) => {
      return Promise.all(value.map(contract => {
        /** @type {import('@web3-storage/data-segment').LegacyPieceLink} */
        const legacyPieceCid = parseLink(key)

        return dealStore.put({
          ...contract,
          // @ts-expect-error not PieceCIDv2
          piece: legacyPieceCid,
          provider: `${contract.provider}`,
          insertedAt: (new Date()).toISOString()
        })
      }))
    })
  )

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
 * @param {OracleContracts} context.previousOracleContracts 
 * @param {OracleContracts} context.updatedOracleContracts 
 */
export function computeDiffOracleState ({ previousOracleContracts, updatedOracleContracts }) {
  /** @type {OracleContracts} */
  const diff = new Map()

  for (const [pieceCid, contracts] of updatedOracleContracts.entries() ) {
    const previousContracts = previousOracleContracts.get(pieceCid) || []
    // Find diff when different length
    if (contracts.length !== previousContracts.length) {
      const diffContracts = []
      // Get contracts for PieceCID still not recorded
      for (const c of contracts) {
        if (!previousContracts.find(pc => pc.dealId === c.dealId)) {
          diffContracts.push(c)
        }
      }
      diff.set(pieceCid, diffContracts)
    }
  }

  return diff
}

/**
 * @param {object} context
 * @param {SpadeOracleStore} context.spadeOracleStore
 * @param {string} context.spadeOracleId
 * @returns {Promise<import('../types').Result<OracleContracts, Error>>}
 */
export async function getSpadeOracleState ({ spadeOracleStore, spadeOracleId }) {
  const getRes = await spadeOracleStore.get(spadeOracleId)
  if (getRes.error) {
    return getRes
  }

  return {
    ok: new Map(Object.entries(decode(getRes.ok.value))),
  }
}

/**
 * @param {object} context
 * @param {SpadeOracleStore} context.spadeOracleStore
 * @param {string} context.spadeOracleId
 * @param {OracleContracts} context.oracleContracts 
 */
async function putUpdatedSpadeOracleState ({ spadeOracleStore, spadeOracleId, oracleContracts }) {
  const putRes = await spadeOracleStore.put({
    key: spadeOracleId,
    value: encode(Object.fromEntries(oracleContracts))
  })

  return putRes
}

/**
 * @param {URL} spadeOracleUrl
 * @returns {Promise<import('../types').Result<OracleContracts, Error>>}
 */
async function getSpadeOracleCurrentState (spadeOracleUrl) {
  /** @type {OracleContracts} */
  const dealMap = new Map()
  const res = await fetch(spadeOracleUrl)
  if (!res.ok) {
    return {
      // TODO: Error
      error: new Error('could not read')
    }
  }

  const resDecompressed = await streamReadAll(
    // @ts-expect-error aws types...
    Readable.fromWeb(res.body)
      .pipe(ZSTDDecompress())
  )
  /** @type {SpadeOracle} */
  const SpadeOracle = JSON.parse(toString(resDecompressed))
  for (const replica of SpadeOracle.active_replicas) {
    // Convert PieceCidV1 to PieceCidV2
    const piecCid = convertPieceCidV1toPieceCidV2(
      parseLink(replica.piece_cid),
      replica.piece_log2_size
    )
    dealMap.set(piecCid.toString(), replica.contracts.map(c => ({
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
  const piece = Piece.fromInfo({
    link,
    size: Piece.Size.fromHeight(height)
  })

  return piece.link
}
