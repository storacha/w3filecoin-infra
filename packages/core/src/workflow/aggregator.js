import { Aggregate, Index } from '@web3-storage/data-segment'

/**
 * @param {object} props
 * @param {import('../types.js').PieceQueue} props.pieceQueue
 * @param {import('../types.js').AggregateQueue} props.aggregateQueue
 * @param {import('@web3-storage/data-segment').PaddedPieceSize} [props.builderSize]
 * @returns {import('../types.js').AggregatorWorkflowResponse}
 */
export async function run ({ pieceQueue, aggregateQueue, builderSize }) {
  // Get maximum number of pieces that can make it to an aggregate
  const size = builderSize || Aggregate.DEFAULT_DEAL_SIZE
  const limit = Index.maxIndexEntriesInDeal(size)

  // Get piece list and immediately leave if no pieces
  const pieceListResponse = await pieceQueue.peek({
    // TODO: We need to see how this behaves with large items
    // Likely paginates...
    limit
  })
  if (pieceListResponse.error) {
    return { error: pieceListResponse.error }
  } else if (!pieceListResponse.ok.length) {
    return {
      ok: {
        count: 0
      }
    }
  }

  // Attempt to create aggregate
  const aggregate = buildAggregate(pieceListResponse.ok, {
    size
  })

  // Write to aggregate queue
  const { error } = await aggregateQueue.put(aggregate)
  if (error) {
    return { error }
  }

  return {
    ok: {
      count: 1
    }
  }  
}

/**
 * @param {import('../types.js').Inserted<import('../types.js').Inclusion>[]} unsortedPieces
 * @param {object} [options]
 * @param {import('@web3-storage/data-segment').PaddedPieceSize} [options.size]
 */
function buildAggregate (unsortedPieces, options = {}) {
  const size = options.size
  const builder = Aggregate.createBuilder({
    size
  })
  // Sort pieces by size as naive implementation of an aggregate
  const pieces = [
    ...unsortedPieces
  ].sort((a, b) => Number(a.size - b.size))
  const addedPieces = []

  for (const item of pieces) {
    try {
      builder.write({
        root: item.piece.multihash.digest,
        size: item.size
      })
      addedPieces.push(item.piece)
    } catch {}
  }

  // TODO: Figure out https://github.com/web3-storage/data-segment/pull/10#discussion_r1261081931
  const aggregate = builder.build()

  return {
    link: aggregate.link(),
    size: aggregate.size,
    pieces: addedPieces
  }
}
