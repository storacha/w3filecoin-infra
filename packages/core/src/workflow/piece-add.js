import { Aggregator } from '@web3-storage/filecoin-client'
import * as Server from '@ucanto/server'
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
 * @param {import('@web3-storage/filecoin-api/types').Queue<Piece>} props.queueClient
 * @param {import('@web3-storage/filecoin-client/types').InvocationConfig} props.invocationConfig
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

  let responses
  try {
    responses = await Promise.all(
      decodedRecords.map(record => addPiece({
        queueClient,
        invocationConfig,
        aggregatorServiceConnection,
        record
      }))
    )
  } catch {
    return {
      error: new AggregateAddFailed('failed to add pieces')
    }
  }

  return {
    ok: {
      // number of successful handled pieces
      countSuccess: responses.reduce((acc, value) => {
        return value.ok ? acc + 1 : acc
      }, 0),
      // failed pieces that can be re-queued
      batchItemFailures: responses.reduce((filtered, value) => {
        if (value.error) {
          filtered.push(value.id)
        }

        return filtered
      }, /** @type {string[]} */ ([]))
    }
  }
}

/**
 * @param {object} props
 * @param {import('@web3-storage/filecoin-api/types').Queue<Piece>} props.queueClient
 * @param {import('@web3-storage/filecoin-client/types').InvocationConfig} props.invocationConfig
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
    { connection: aggregatorServiceConnection }
  )
  if (aggregateAddResponse.out.error) {
    return {
      id: record.id,
      error: aggregateAddResponse.out.error
    }
  }

  const queueAddResponse = await queueClient.add(record.piece)
  if (queueAddResponse.error) {
    return {
      id: record.id,
      error: queueAddResponse.error
    }
  }

  return {
    id: record.id,
    ok: {}
  }
}

export const AggregateAddErrorName = /** @type {const} */ (
  'AggregateAddFailed'
)
export class AggregateAddFailed extends Server.Failure {
  get reason() {
    return this.message
  }

  get name() {
    return AggregateAddErrorName
  }
}
