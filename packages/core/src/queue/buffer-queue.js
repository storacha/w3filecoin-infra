import { encode as JSONencode, decode as JSONdecode } from '@ipld/dag-json'
import { toString } from 'uint8arrays/to-string'
import { fromString } from 'uint8arrays/from-string'

import { createQueueClient } from './client.js'

/**
 * @typedef {import('@web3-storage/filecoin-api/aggregator/api').BufferMessage} BufferMessage
 * @typedef {import('./client.js').ClientEncodedMessage} ClientEncodedMessage
 */

/**
 * @param {BufferMessage} bufferMessage
 * @param {import('./types.js').QueueOptions} options
 * @returns {ClientEncodedMessage}
 */
const encodeMessage = (bufferMessage, options = {}) => {
  const encodedBytes = JSONencode(bufferMessage)
  return {
    MessageBody: toString(encodedBytes),
    // FIFO Queue message group id
    MessageGroupId: options.disableMessageGroupId ? undefined : bufferMessage.group
  }
}

/**
 * @param {{ 'MessageBody': string }} message 
 * @returns {BufferMessage}
 */
export const decodeMessage = (message) => {
  const decodedBytes = fromString(message.MessageBody)
  return JSONdecode(decodedBytes)
}

/**
 * 
 * @param {import('./types.js').QueueConnect | import('@aws-sdk/client-sqs').SQSClient} conf
 * @param {object} context
 * @param {string} context.queueUrl
 * @param {boolean} [context.disableMessageGroupId]
 * @returns {import('@web3-storage/filecoin-api/aggregator/api').BufferQueue}
 */
export function createClient (conf, context) {
  return createQueueClient(conf,
    {
      queueUrl: context.queueUrl,
      encodeMessage: (item) => encodeMessage(item, {
        disableMessageGroupId: context.disableMessageGroupId
      })
    })
}
