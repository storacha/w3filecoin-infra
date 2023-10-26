import { Aggregator } from '@web3-storage/filecoin-client-legacy'
import { decode as decodePiece } from '../data/piece.js'

/**
 * @typedef {import('@web3-storage/data-segment').PieceLink} PieceLink
 * @typedef {import('../data/types.js').Piece<PieceLink>} Piece
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
 * Invokes aggregate/add on the queued system to go through the backlog of pieces provided by Storefront actors.
 * Once the `aggregate/add` succeeds, the peice is queued for buffering on its way to be aggregated.
 * When only a subset of the pieces succeed, a `batchItemFailures` is returned so that these items can be re-queued.
 *
 * @param {object} props
 * @param {import('@web3-storage/filecoin-api-legacy/types').Queue<Piece>} props.queueClient
 * @param {import('@web3-storage/filecoin-client-legacy/types').InvocationConfig} props.invocationConfig
 * @param {import('@ucanto/principal/ed25519').ConnectionView<any>} props.aggregatorServiceConnection
 * @param {EncodedRecord[]} props.records - message encoded piece records
 */
export async function addPieces ({
  queueClient,
  invocationConfig,
  aggregatorServiceConnection,
  records,
}) {
  const decodedRecords = await Promise.all(
    records.map(async pr => ({
      piece: await decodePiece.message(pr.body),
      id: pr.id
    }))
  )

  const responses = await Promise.all(
    decodedRecords.map(record => addPiece({
      queueClient,
      invocationConfig,
      aggregatorServiceConnection,
      record
    }))
  )

  const failedResponses = responses.filter(r => Boolean(r.error))
  if (failedResponses.length) {
    return {
      error: failedResponses.map(r => r.error)
    }
  }

  return {
    ok: {}
  }
}

/**
 * @param {object} props
 * @param {import('@web3-storage/filecoin-api-legacy/types').Queue<Piece>} props.queueClient
 * @param {import('@web3-storage/filecoin-client-legacy/types').InvocationConfig} props.invocationConfig
 * @param {import('@ucanto/principal/ed25519').ConnectionView<any>} props.aggregatorServiceConnection
 * @param {Record} props.record 
 */
async function addPiece ({
  queueClient,
  invocationConfig,
  aggregatorServiceConnection,
  record
}) {
  const aggregateAddResponse = await Aggregator.aggregateAdd(
    invocationConfig,
    record.piece.piece,
    record.piece.storefront,
    record.piece.group,
    // @ts-expect-error different ucanto versions
    { connection: aggregatorServiceConnection }
  )
  if (aggregateAddResponse.out.error) {
    return {
      error: {
        cause: aggregateAddResponse.out.error,
        id: record.id,
      }
    }
  }

  const queueAddResponse = await queueClient.add(record.piece)
  if (queueAddResponse.error) {
    return {
      error: {
        cause: queueAddResponse.error,
        id: record.id,
      }
    }
  }

  return {
    ok: {  id: record.id }
  }
}
