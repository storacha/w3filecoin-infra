import * as Server from '@ucanto/server'

import { decode as decodePiece } from '../data/piece.js'
import { getMessageGroupId } from '../utils.js'

/**
 * @typedef {import('@web3-storage/data-segment').PieceLink} PieceLink
 * @typedef {import('../data/types.js').Piece<PieceLink>} Piece
 * @typedef {import('../data/types.js').Buffer<PieceLink>} Data
 * @typedef {import('../data/types.js').PiecePolicy} PiecePolicy
 * 
 * @typedef {object} EncodedRecord
 * @property {string} body
 * @property {string} id
 * 
 * @typedef {object} Record
 * @property {Piece} piece
 * @property {string} id
 */

/**
 * @param {object} props
 * @param {import('@web3-storage/filecoin-api/types').Store<Data>} props.storeClient 
 * @param {import('@web3-storage/filecoin-api/types').Queue<Data>} props.queueClient
 * @param {EncodedRecord[]} props.records - message encoded piece records
 * @param {boolean} [props.disableMessageGroupId] - only supported in FIFO queues
 */
export async function bufferPieces ({
  storeClient,
  queueClient,
  records,
  disableMessageGroupId,
}) {
  // Decode records
  const decodedRecords = await Promise.all(
    records.map(async r => ({
      piece: await decodePiece.message(r.body),
      id: r.id
    }))
  )

  // Split records by the group they belong to
  const groupedRecords = groupByStorefrontIdentifier(decodedRecords)

  // Buffer records by groups
  let responses
  try {
    responses = await Promise.all(
      [...groupedRecords.entries()].map(([messageGroupId, records]) => bufferGroupPieces({
        storeClient,
        queueClient,
        records,
        messageGroupId: disableMessageGroupId ? undefined : messageGroupId
      }))
    )
  } catch {
    return {
      error: new PieceBufferingFailed('failed to buffer given pieces')
    }
  }

  // number of successful handled pieces
  const countSuccess = responses.reduce((acc, value) => {
    return value.ok ? acc + value.ok : acc
  }, 0)

  // Send back first error given no partial success was achieved
  if (!countSuccess) {
    return {
      error: responses.find(r => r.error)?.error || new PieceBufferingFailed()
    }
  }

  return {
    ok: {
      countSuccess,
      // failed pieces that can be re-queued
      batchItemFailures: responses.reduce((conc, value) => {
        if (value.error) {
          conc = [
            ...conc,
            ...value.batchItemFailures
          ]
        }

        return conc
      }, /** @type {string[]} */ ([]))
    }
  }
}

/**
 * @param {object} props
 * @param {import('@web3-storage/filecoin-api/types').Store<Data>} props.storeClient 
 * @param {import('@web3-storage/filecoin-api/types').Queue<Data>} props.queueClient
 * @param {Record[]} props.records
 * @param {string} [props.messageGroupId]
 */
async function bufferGroupPieces ({
  storeClient,
  queueClient,
  records,
  messageGroupId,
}) {
  // Get storefront and group from one of the pieces
  // per grouping, all of them are the same
  const { storefront, group } = records[0].piece

  // Create buffer
  const buffer = {
    pieces: records.map(r => ({
      ...r.piece,
      // set policy as insertion
      policy: /** @type {PiecePolicy} */ (0)
    })).sort(),
    storefront,
    group
  }

  const bufferStored = await storeClient.put(buffer)
  if (bufferStored.error) {
    return {
      batchItemFailures: records.map(r => r.id),
      error: bufferStored.error
    }
  }
  const bufferQueued = await queueClient.add(buffer, {
    // only available in FIFO queues
    messageGroupId
  })

  if (bufferQueued.error) {
    return {
      batchItemFailures: records.map(r => r.id),
      error: bufferQueued.error
    }
  }
  
  return {
    ok: records.length,
  }
}

/**
 * only FIFO queues allow message group id, so we need to split them here.
 *
 * @param {Record[]} records
 */
function groupByStorefrontIdentifier (records) {
  return records.reduce((acc, cur) => {
    const messageGroupId = getMessageGroupId(cur.piece.storefront, cur.piece.group)

    acc.set(messageGroupId, [
      ...acc.get(messageGroupId) || [],
      cur
    ])

    return acc
  }, /** @type {Map<string, Record[]>} */ (new Map()))
}

export const PieceBufferingErrorName = /** @type {const} */ (
  'PieceBufferingFailed'
)
export class PieceBufferingFailed extends Server.Failure {
  get reason() {
    return this.message
  }

  get name() {
    return PieceBufferingErrorName
  }
}
