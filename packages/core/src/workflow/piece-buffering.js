import { decode as decodePiece } from '../data/piece.js'

/**
 * @typedef {import('@web3-storage/data-segment').PieceLink} PieceLink
 * @typedef {import('../data/types.js').Buffer<PieceLink>} Data
 * @typedef {import('../data/types.js').PiecePolicy} PiecePolicy
 */

/**
 * @param {object} props
 * @param {import('@web3-storage/filecoin-api/types').Store<Data>} props.storeClient 
 * @param {import('@web3-storage/filecoin-api/types').Queue<Data>} props.queueClient
 * @param {string[]} props.pieceRecords 
 * @param {string} [props.groupId]
 */
export async function bufferPieces ({
  storeClient,
  queueClient,
  pieceRecords,
  groupId
}) {
  // Get storefront and group from one of the pieces
  // per grouping, all of them are the same
  const { storefront, group } = await decodePiece.message(pieceRecords[0])

  // Create buffer
  const buffer = {
    pieces: (await Promise.all(pieceRecords.map(async (piece) => {
      const entry = await decodePiece.message(piece)
      return {
        ...entry,
        // set policy as insertion
        policy: /** @type {PiecePolicy} */ (0)
      }
      // TODO: we need to sort by size
    }))).sort(),
    storefront,
    group
  }
  const bufferStored = await storeClient.put(buffer)
  if (bufferStored.error) {
    return {
      error: bufferStored.error
    }
  }

  const bufferQueued = await queueClient.add(buffer, {
    messageGroupId: groupId
  })
  if (bufferQueued.error) {
    return {
      error: bufferQueued.error
    }
  }
  
  return {
    ok: pieceRecords.length
  }
}
