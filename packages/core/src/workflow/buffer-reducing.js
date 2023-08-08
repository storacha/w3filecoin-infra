import { Aggregate, Piece, NODE_SIZE, Index } from '@web3-storage/data-segment'

import { decode as decodeBuffer, encodeBlock } from '../data/buffer.js'

/**
 * @typedef {import('@web3-storage/data-segment').PieceLink} PieceLink
 * @typedef {import('@web3-storage/data-segment').PieceInfo} PieceInfo
 * @typedef {import('@web3-storage/data-segment').LegacyPieceLink} LegacyPieceLink
 * @typedef {import('../data/types.js').Buffer<PieceLink>} Buffer
 * @typedef {import('../data/types.js').BufferedPiece<PieceLink>} BufferedPiece
 * @typedef {import('../data/types.js').Aggregate<LegacyPieceLink>} Aggregate
 * @typedef {import('@web3-storage/filecoin-api/types').StoreGetError} StoreGetError
 * @typedef {{ bufferedPieces: BufferedPiece[], storefront: string, group: string }} GetBufferedPieces
 * @typedef {import('../types.js').Result<GetBufferedPieces, StoreGetError>} GetBufferedPiecesResult
 * 
 * @typedef {object} AggregateInfo
 * @property {BufferedPiece[]} addedBufferedPieces
 * @property {BufferedPiece[]} remainingBufferedPieces
 * @property {PieceInfo} aggregate
 */

/**
 * @param {object} props
 * @param {import('@web3-storage/filecoin-api/types').Store<Buffer>} props.storeClient 
 * @param {import('@web3-storage/filecoin-api/types').Queue<Buffer>} props.bufferQueueClient
 * @param {import('@web3-storage/filecoin-api/types').Queue<Aggregate>} props.aggregateQueueClient
 * @param {string[]} props.bufferRecords
 * @param {number} props.maxAggregateSize
 * @param {number} props.minAggregateSize
 * @param {string} [props.groupId]
 */
export async function reduceBuffer ({
  storeClient,
  bufferQueueClient,
  aggregateQueueClient,
  bufferRecords,
  maxAggregateSize,
  minAggregateSize,
  groupId
}) {
  // Get reduced buffered pieces
  const {
    error: errorGetBufferedPieces,
    ok: okGetBufferedPieces,
  } = await getBufferedPieces(bufferRecords, storeClient)
  if (errorGetBufferedPieces) {
    return { error: errorGetBufferedPieces }
  }
  const { bufferedPieces, storefront, group } = okGetBufferedPieces

  // Attempt to aggregate buffered pieces within the ranges.
  // In case it is possible, included pieces and remaining pieces are returned
  // so that they can be propagated to respective stores/queues.
  const aggregateInfo = aggregatePieces(bufferedPieces, {
    maxAggregateSize,
    minAggregateSize
  })

  // Store buffered pieces if not enough to do aggregate and re-queue them
  if (!aggregateInfo) {
    const {
      error: errorHandleBufferReducingWithoutAggregate
    } = await handleBufferReducingWithoutAggregate({
      buffer: {
        pieces: bufferedPieces,
        storefront,
        group
      },
      storeClient,
      bufferQueueClient,
      groupId
    })

    if (errorHandleBufferReducingWithoutAggregate) {
      return { error : errorHandleBufferReducingWithoutAggregate}
    }

    // No pieces were aggregate
    return { ok: 0 }
  }

  // Store buffered pieces to do aggregate and re-queue remaining ones
  const {
    error: errorHandleBufferReducingWithAggregate
  } = await handleBufferReducingWithAggregate({
    aggregateInfo,
    storeClient,
    bufferQueueClient,
    aggregateQueueClient,
    storefront,
    group,
    groupId
  })

  if (errorHandleBufferReducingWithAggregate) {
    return { error : errorHandleBufferReducingWithAggregate}
  }

  return {
    ok: aggregateInfo.addedBufferedPieces.length
  }
}

/**
 * @param {object} props
 * @param {AggregateInfo} props.aggregateInfo
 * @param {import('@web3-storage/filecoin-api/types').Store<Buffer>} props.storeClient
 * @param {import('@web3-storage/filecoin-api/types').Queue<Aggregate>} props.aggregateQueueClient
 * @param {import('@web3-storage/filecoin-api/types').Queue<Buffer>} props.bufferQueueClient
 * @param {string} props.storefront
 * @param {string} props.group
 * @param {string} [props.groupId]
 */
async function handleBufferReducingWithAggregate ({
  aggregateInfo,
  storeClient,
  aggregateQueueClient,
  bufferQueueClient,
  storefront,
  group,
  groupId
}) {
  // If aggregate has enough space
  // store buffered pieces that are part of aggregate and queue aggregate
  // store remaining pieces and queue them to be reduced
  const aggregateReducedBuffer = {
    pieces: aggregateInfo.addedBufferedPieces,
    storefront,
    group
  }

  // Store buffered pieces for aggregate
  const {
    error: errorStoreAggregateBufferedPieces,
    ok: okStoreAggregateBufferedPieces
  } = await storeBufferedPieces(aggregateReducedBuffer, storeClient)

  if (errorStoreAggregateBufferedPieces) {
    return { error: errorStoreAggregateBufferedPieces }
  }

  // Queue buffered pieces to aggregate
  const aggregateRecord = {
    piece: aggregateInfo.aggregate.link,
    buffer: okStoreAggregateBufferedPieces,
    insertedAt: Date.now(),
    storefront,
    group,
    stat: /** @type {import('../data/types.js').AggregateStatus} */ (0),
  }
  const aggregateQueued = await aggregateQueueClient.add(aggregateRecord, {
    messageGroupId: groupId
  })
  if (aggregateQueued.error) {
    return {
      error: aggregateQueued.error
    }
  }

  // Store remaining buffered pieces to reduce
  const remainingReducedBuffer = {
    pieces: aggregateInfo.remainingBufferedPieces,
    storefront,
    group
  }
  const {
    error: errorStoreRemainingBufferedPieces,
  } = await storeBufferedPieces(remainingReducedBuffer, storeClient)

  if (errorStoreRemainingBufferedPieces) {
    return { error: errorStoreAggregateBufferedPieces }
  }

  // queue remaining buffered pieces
  const bufferQueuedToReduce = await bufferQueueClient.add(remainingReducedBuffer, {
    messageGroupId: groupId
  })
  if (bufferQueuedToReduce.error) {
    return {
      error: bufferQueuedToReduce.error
    }
  }

  return { ok: {} }
}

/**
 * Store given buffer into store and queue it to further reducing.
 *
 * @param {object} props
 * @param {Buffer} props.buffer 
 * @param {import('@web3-storage/filecoin-api/types').Store<Buffer>} props.storeClient 
 * @param {import('@web3-storage/filecoin-api/types').Queue<Buffer>} props.bufferQueueClient 
 * @param {string} [props.groupId]
 */
async function handleBufferReducingWithoutAggregate ({
  buffer,
  storeClient,
  bufferQueueClient,
  groupId
}) {
  const {
    error: errorStoreBufferedPieces
  } = await storeBufferedPieces(buffer, storeClient)

  if (errorStoreBufferedPieces) {
    return { error: errorStoreBufferedPieces}
  }

  const bufferQueuedToReduce = await bufferQueueClient.add(buffer, {
    messageGroupId: groupId
  })
  if (bufferQueuedToReduce.error) {
    return {
      error: bufferQueuedToReduce.error
    }
  }

  return { ok: {} }
}

/**
 * @param {Buffer} buffer
 * @param {import('@web3-storage/filecoin-api/types').Store<Buffer>} storeClient
 */
async function storeBufferedPieces (buffer, storeClient) {
  const bufferStored = await storeClient.put(buffer)
  if (bufferStored.error) {
    return {
      error: bufferStored.error
    }
  }
  const bufferCid = await encodeBlock(buffer)

  return {
    ok: bufferCid.cid
  }
}

/**
 * Attempt to build an aggregate with buffered pieces within ranges.
 *
 * @param {BufferedPiece[]} bufferedPieces
 * @param {object} sizes
 * @param {number} sizes.maxAggregateSize
 * @param {number} sizes.minAggregateSize
 */
function aggregatePieces (bufferedPieces, sizes) {
  // Create builder with maximum size and try to fill it up
  const builder = Aggregate.createBuilder({
    size: Piece.PaddedSize.from(sizes.maxAggregateSize)
  })

  // add pieces to an aggregate until there is no more space, or no more pieces
  /** @type {BufferedPiece[]} */
  const addedBufferedPieces = []
  /** @type {BufferedPiece[]} */
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
  const totalUsedSpace = builder.offset * BigInt(NODE_SIZE) + BigInt(builder.limit) * BigInt(Index.EntrySize) 

  // If not enough space return undefined
  if (totalUsedSpace > BigInt(sizes.minAggregateSize)) {
    return
  }

  const aggregate = builder.build()

  return {
    addedBufferedPieces,
    remainingBufferedPieces,
    aggregate: aggregate.toInfo()
  }
}

/**
 * Get buffered pieces from queue buffer records.
 *
 * @param {string[]} bufferRecords
 * @param {import('@web3-storage/filecoin-api/types').Store<Buffer>} storeClient
 * @returns {Promise<GetBufferedPiecesResult>}
 */
async function getBufferedPieces (bufferRecords, storeClient) {
  const bufferReferences = await Promise.all(
    bufferRecords.map((message) => getBuffer(message, storeClient))
  )

  // Check if one of the buffers failed to get
  const bufferReferenceGetError = bufferReferences.find(get => get.error)
  if (bufferReferenceGetError?.error) {
    return {
      error: bufferReferenceGetError.error
    }
  }

  // @ts-expect-error typescript does not understand with find that no error
  const { storefront, group } = bufferReferences[0].ok

  // Concatenate pieces and sort them by policy and size
  /** @type {BufferedPiece[]} */
  const bufferedPieces = []
  for (const b of bufferReferences) {
    // eslint-disable-next-line unicorn/prefer-spread
    bufferedPieces.concat(b.ok?.pieces || [])
  }
  bufferedPieces.sort(sortPieces)

  return {
    ok: {
      bufferedPieces,
      storefront,
      group
    }
  }
}

/**
 * Get buffer from queue message.
 *
 * @param {string} messageBody
 * @param {import('@web3-storage/filecoin-api/types').Store<Buffer>} storeClient
 */
async function getBuffer (messageBody, storeClient) {
  const bufferRef = await decodeBuffer.message(messageBody)
  const getBufferRes = await storeClient.get(
    `${bufferRef.cid}/${bufferRef.cid}`
  )

  return getBufferRes
}

/**
 * Sort given buffered pieces by policy and then by size.
 *
 * @param {BufferedPiece} a
 * @param {BufferedPiece} b
 */
export function sortPieces (a, b) {
  return a.policy !== b.policy ?
    a.policy - b.policy :
    Piece.fromLink(a.piece).height - Piece.fromLink(b.piece).height
}
