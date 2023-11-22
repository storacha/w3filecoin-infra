import { parse as parseLink } from 'multiformats/link'
import { convertPieceCidV1toPieceCidV2 } from '@w3filecoin/core/src/utils.js'

const pieceString = process.argv[2]
if (!pieceString) {
  throw new Error('no piece was provided')
}

const height = process.argv[3]
if (!height) {
  throw new Error('no height was provided')
}

/** @type {import('@web3-storage/data-segment').LegacyPieceLink} */
let pieceCidV1
try {
  pieceCidV1 = parseLink(pieceString)
} catch {
  throw new Error(`PieceCIDv1 received ${pieceString} is not a valid CID`)
}

const piece = convertPieceCidV1toPieceCidV2(
  pieceCidV1,
  Number(height)
)

console.log(`PieceCIDv1 ${pieceString} with height ${height} corresponds to PieceCIDv2 ${piece}`)
