import { Piece } from '@web3-storage/data-segment'

/**
 * @param {string} storefront
 * @param {string} group
 */
export function getMessageGroupId (storefront, group) {
  return `${storefront}:${group}`
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

/**
 * 
 * @param {number} log2Size 
 */
export function log2PieceSizeToHeight (log2Size) {
  return Piece.Size.Expanded.toHeight(2n ** BigInt(log2Size))
}
