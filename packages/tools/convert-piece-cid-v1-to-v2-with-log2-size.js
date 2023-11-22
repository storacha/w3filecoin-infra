import { parse as parseLink } from 'multiformats/link'
import { log2PieceSizeToHeight, convertPieceCidV1toPieceCidV2 } from '@w3filecoin/core/src/utils.js'

const pieceString = process.argv[2]
if (!pieceString) {
  throw new Error('no piece was provided')
}

const log2Size = process.argv[3]
if (!log2Size) {
  throw new Error('no log2 size was provided')
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
  log2PieceSizeToHeight(Number(log2Size))
)

console.log(`PieceCIDv1 ${pieceString} with log2 size ${log2Size} corresponds to PieceCIDv2 ${piece}`)
