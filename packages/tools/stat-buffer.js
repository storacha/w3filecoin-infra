import { Aggregate, Piece, NODE_SIZE, Index } from '@web3-storage/data-segment'
import { parseLink } from '@ucanto/server'
import { createClient as createBufferStoreClient } from '@w3filecoin/core/src/store/aggregator-buffer-store.js'

const AWS_REGION = 'us-west-2'
const config = {
  maxAggregateSize: 2**35,
}

const bufferCidString = process.argv[2]
if (!bufferCidString) {
  throw new Error('no buffer CID string was provided')
}

const bufferStore = createBufferStoreClient({
  region: AWS_REGION
}, {
  name: 'prod-w3filecoin-aggregator-buffer-store-0'
})

const bufferCid = parseLink(bufferCidString)

// @ts-expect-error CID multiple versions can exist
const buffer = await bufferStore.get(bufferCid)
if (buffer.error) {
  throw new Error(`Error getting buffer: ${buffer.error.message}`)
}

const bufferedPieces = buffer.ok.buffer.pieces
const bufferUtilizationSize = bufferedPieces.reduce((total, p) => {
  const piece = Piece.fromLink(p.piece)
  total += piece.size
  return total
}, 0n)

console.log('Number of piece:', bufferedPieces.length)
console.log('Total size of pieces:', bufferUtilizationSize)

// Create builder with maximum size and try to fill it up
const builder = Aggregate.createBuilder({
  size: Aggregate.Size.from(config.maxAggregateSize),
})

// add pieces to an aggregate until there is no more space, or no more pieces
const addedBufferedPieces = []
const remainingBufferedPieces = []

for (const bufferedPiece of bufferedPieces) {
  const p = Piece.fromLink(bufferedPiece.piece)
  if (builder.estimate(p).error) {
    remainingBufferedPieces.push(bufferedPiece)
    continue
  }
  builder.write(p)
  addedBufferedPieces.push(bufferedPiece)
}

const totalUsedSpace =
  builder.offset * BigInt(NODE_SIZE) +
  BigInt(builder.limit) * BigInt(Index.EntrySize)

console.log('Total aggregate used space:', totalUsedSpace)
