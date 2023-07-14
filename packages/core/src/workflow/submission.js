import { SendMessageCommand } from '@aws-sdk/client-sqs'
import { encode as JSONencode, decode as JSONdecode } from '@ipld/dag-json'
import { fromString } from 'uint8arrays/from-string'
import { toString } from 'uint8arrays/to-string'
import * as ed25519 from '@ucanto/principal/ed25519'
import * as DID from '@ipld/dag-ucan/did'
import { Aggregate } from '@web3-storage/aggregate-client'

import { SqsSendMessageError } from './errors.js'

/**
 * @param {import('../types').Aggregate} submissionItem 
 * @returns {string}
 */
export const encode = (submissionItem) => {
  const encodedBytes = JSONencode(submissionItem)

  return toString(encodedBytes)
}

/**
 * @param {string} submissionItem
 * @returns {import('../types').Aggregate}
 */
export const decode = (submissionItem) => {
  const decodedBytes = fromString(submissionItem)
  return JSONdecode(decodedBytes)
}

/**
 * Reads queued content from the given `aggregateQueue` and sends each to
 * a given SQS queue.
 *
 * It can fail returning error `DatabaseOperationError` or `SqsSendMessageError`.
 * User can rely on these to try different infrastructure resources, or simply
 * use different status codes for error monitoring.
 *
 * @param {object} props
 * @param {import('../types.js').AggregateQueue} props.aggregateQueue
 * @param {import('@aws-sdk/client-sqs').SQSClient} props.sqsClient
 * @param {string} props.queueUrl
 * @returns {import('../types.js').ConsumerWorkflowResponse}
 */
export async function consume ({ aggregateQueue, sqsClient, queueUrl }) {
  const aggregateListResponse = await aggregateQueue.peek({
    limit: 100
  })
  if (aggregateListResponse.error) {
    return { error: aggregateListResponse.error }
  }

  try {
    for (const aggregate of aggregateListResponse.ok) {
      // Deduplicates messages with the same ID to not invoke unecessary lambdas
      // through `contentBasedDeduplication` in SQS configuration.
      // However, operations are idempotent and in case they happen to be executed
      // after the message deduplication id timesout is also acceptable.
      const msgCommand = new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: encode(aggregate),
      })
  
      await sqsClient.send(msgCommand)
    }
  } catch (/** @type {any} */ error) {
    return { error: new SqsSendMessageError(error.message) }
  }

  return {
    ok: { count: aggregateListResponse.ok.length }
  }
}

/**
 * @param {object} props
 * @param {string} props.item
 * @param {import('../types.js').DealQueue} props.dealQueue
 * @param {import('../types.js').DatabaseView} props.databaseView
 * @param {string} props.did
 * @param {string} props.privateKey
 * @param {ed25519.ConnectionView<any>} props.aggregationServiceConnection
 * @returns {import('../types.js').SubmissionWorkflowResponse}
 */
export async function buildOffer (props) {
  const { item, dealQueue, databaseView, did, privateKey, aggregationServiceConnection } = props
  const aggregate = decode(item)
  const issuer = getStorefrontSigner({ did, privateKey })
  const audience = aggregationServiceConnection.id
  /** @type {import('@web3-storage/aggregate-client/types').InvocationConfig} */
  const InvocationConfig = {
    issuer,
    audience,
    with: issuer.did(),
  }

  const pieces = await databaseView.selectCargoIncluded(aggregate.link)
  if (pieces.error) {
    return { error: pieces.error }
  }
  // TODO: remove cast
  /** @type {import('@web3-storage/aggregate-client/types').Piece[]}  */
  const aggregateInclusion = pieces.ok.map(piece => ({
    link: piece.piece,
    size: Number(piece.size)
  }))

  const res = await Aggregate.aggregateOffer(
    InvocationConfig,
    {
      link: aggregate.link,
      // TODO: remove cast
      size: Number(aggregate.size)
    },
    aggregateInclusion,
    { connection: aggregationServiceConnection }
  )

  if (res.out.error) {
    return {
      error: res.out.error
    }
  }

  // Add to deal queue
  await dealQueue.put({
    aggregate: aggregate.link
  })

  return {
    ok: {}
  }
}

/**
 * @param {import('../types').StorefrontSignerCtx} config 
 */
function getStorefrontSigner(config) {
  const signer = ed25519.parse(config.privateKey)
  if (config.did) {
    const did = DID.parse(config.did).did()
    return signer.withDID(did)
  }
  return signer
}
