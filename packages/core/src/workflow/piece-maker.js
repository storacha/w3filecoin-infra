import { SendMessageCommand } from '@aws-sdk/client-sqs'
import { CommP } from '@web3-storage/data-segment'

import { SqsSendMessageError } from './errors.js'

/**
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
      const msgCommand = new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify({
          ...content,
          link: content.link.toString()
        })
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
 * @param {import('../types.js').Content} props.item
 * @param {import('../types.js').PieceQueue} props.pieceQueue
 * @param {import('../types.js').ContentResolver} props.contentResolver
 * @returns {import('../types.js').ProducerWorkflowResponse}
 */
export async function producer ({ item, pieceQueue, contentResolver }) {
  // TODO: we can consider checking if already in the destination queue
  // before doing the precessing

  const { ok: bytes, error: contentResolverError } = await contentResolver.resolve(item)
  if (contentResolverError) {
    return { error: contentResolverError }
  }

  const commP = CommP.build(bytes)
  const { error } = await pieceQueue.put({
    link: commP.link(),
    size: commP.pieceSize,
    content: item.link
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
