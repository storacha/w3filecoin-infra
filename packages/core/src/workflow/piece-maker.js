import { SendMessageCommand } from '@aws-sdk/client-sqs'
import { CommP } from '@web3-storage/data-segment'
import { encode as JSONencode, decode as JSONdecode } from '@ipld/dag-json'
import { fromString } from 'uint8arrays/from-string'
import { toString } from 'uint8arrays/to-string'

import { SqsSendMessageError } from './errors.js'

/**
 * @param {import('../types').Content} pieceMakerItem 
 * @returns {string}
 */
export const encode = (pieceMakerItem) => {
  const encodedBytes = JSONencode({
    ...pieceMakerItem,
    // dag-json does not support URL encoding, so manually encode as string
    source: pieceMakerItem.source.map(u => u.toString())
  })

  return toString(encodedBytes)
}

/**
 * @param {string} pieceMakerItem
 * @returns {import('../types').Content}
 */
export const decode = (pieceMakerItem) => {
  const decodedBytes = fromString(pieceMakerItem)
  const decoded = JSONdecode(decodedBytes)
  return {
    ...decoded,
    source: decoded.source.map((/** @type {string} */ u) => new URL(u))
  }
}

/**
 * Reads queued content from the given `contentQueue` and sends each to
 * a given SQS queue.
 *
 * It can fail returning error `DatabaseOperationError` or `SqsSendMessageError`.
 * User can rely on these to try different infrastructure resources, or simply
 * use different status codes for error monitoring.
 *
 * @param {object} props
 * @param {import('../types.js').ContentQueue} props.contentQueue
 * @param {import('@aws-sdk/client-sqs').SQSClient} props.sqsClient
 * @param {string} props.queueUrl
 * @returns {import('../types.js').ConsumerWorkflowResponse}
 */
export async function consumer ({ contentQueue, sqsClient, queueUrl }) {
  const contentListResponse = await contentQueue.peek({
    limit: 100
  })
  if (contentListResponse.error) {
    return { error: contentListResponse.error }
  }

  try {
    for (const content of contentListResponse.ok) {
      // Deduplicates messages with the same ID to not invoke unecessary lambdas
      // through `contentBasedDeduplication` in SQS configuration.
      // However, operations are idempotent and in case they happen to be executed
      // after the message deduplication id timesout is also acceptable.
      const msgCommand = new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: encode(content),
      })
  
      await sqsClient.send(msgCommand)
    }
  } catch (/** @type {any} */ error) {
    return { error: new SqsSendMessageError(error.message) }
  }

  return {
    ok: { count: contentListResponse.ok.length }
  }
}

/**
 * @param {object} props
 * @param {string} props.item
 * @param {import('../types.js').PieceQueue} props.pieceQueue
 * @param {import('../types.js').ContentResolver} props.contentResolver
 * @returns {import('../types.js').ProducerWorkflowResponse}
 */
export async function producer ({ item, pieceQueue, contentResolver }) {
  const content = decode(item)

  // TODO: we can consider checking if already in the destination queue
  // before doing the precessing

  const { ok: bytes, error: contentResolverError } = await contentResolver.resolve(content)
  if (contentResolverError) {
    return { error: contentResolverError }
  }

  const commP = CommP.build(bytes)
  const { error } = await pieceQueue.put({
    link: commP.link(),
    size: commP.pieceSize,
    content: content.link
  })

  if (error) {
    return {
      error
    }
  }

  return {
    ok: {}
  }
}
