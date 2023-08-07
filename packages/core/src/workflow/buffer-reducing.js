import { Aggregate, Piece } from '@web3-storage/data-segment'

import { decode as decodeBuffer, encodeBlock } from '../data/buffer.js'

/**
 * @typedef {import('@web3-storage/data-segment').PieceLink} PieceLink
 * @typedef {import('../data/types.js').Buffer<PieceLink>} Buffer
 * @typedef {import('../data/types.js').BufferedPiece<PieceLink>} BufferedPiece
 * @typedef {import('../data/types.js').Aggregate<PieceLink>} Aggregate
 */

/**
 * @param {object} props
 * @param {import('@web3-storage/filecoin-api/types').Store<Buffer>} props.storeClient 
 * @param {import('@web3-storage/filecoin-api/types').Queue<Buffer>} props.bufferQueueClient
 * @param {import('@web3-storage/filecoin-api/types').Queue<Aggregate>} props.aggregateQueueClient
 * @param {string[]} props.bufferRecords
 * @param {string} [props.groupId]
 */
export async function reduceBuffer ({
  storeClient,
  bufferQueueClient,
  aggregateQueueClient,
  bufferRecords,
  groupId
}) {
  const bufferReferences = await Promise.all(
    bufferRecords.map((message) => getBuffer(message, storeClient))
  )

  // Check if one of the buffers failed to get
  const bufferReferenceGetError = bufferReferences.find(get => get.error)
  if (bufferReferenceGetError) {
    return {
      error: bufferReferenceGetError.error
    }
  }

  // @ts-expect-error typescript does not understand with find that no error
  const { storefront, group } = bufferReferences[0].ok

  /** @type {BufferedPiece[]} */
  const pieces = []
  for (const b of bufferReferences) {
    // eslint-disable-next-line unicorn/prefer-spread
    pieces.concat(b.ok?.pieces || [])
  }

  // TODO: sort pieces by size + policy

  // create combined buffer
  const reducedBuffer = {
    pieces,
    storefront,
    group
  }

  // try to create an aggregate from pieces and check size
  const aggregate = Aggregate.build({
    pieces: pieces.map(p => ({
      link: p.piece,
      // TODO: size should not be needed once encoded, so using random
      size: Piece.PaddedSize.from(100)
    })),
  })

  // TODO: check size

  // TODO: writer

  // TODO: if aggregate is possible, put remaining items back in the queue

  const bufferStored = await storeClient.put(reducedBuffer)
  if (bufferStored.error) {
    return {
      error: bufferStored.error
    }
  }
  const bufferCid = await encodeBlock(reducedBuffer)

  const aggregateRecord = {
    piece: aggregate.link,
    buffer: bufferCid.cid,
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

  return {
    ok: bufferRecords.length
  }
}

/**
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
