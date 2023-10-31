import { encode as JSONencode, decode as JSONdecode } from '@ipld/dag-json'
import { toString } from 'uint8arrays/to-string'
import { fromString } from 'uint8arrays/from-string'

import { createQueueClient } from './client.js'

/**
 * @typedef {import('@web3-storage/filecoin-api/aggregator/api').AggregateOfferMessage} AggregateOfferMessage
 * @typedef {import('./client.js').ClientEncodedMessage} ClientEncodedMessage
 */

/**
 * @param {AggregateOfferMessage} aggregateOfferMessage
 * @param {import('./types.js').QueueOptions} options
 * @returns {ClientEncodedMessage}
 */
const encodeMessage = (aggregateOfferMessage, options) => {
  const encodedBytes = JSONencode(aggregateOfferMessage)
  return {
    MessageBody: toString(encodedBytes),
    // FIFO Queue message group id
    MessageGroupId: options.disableMessageGroupId ? undefined : aggregateOfferMessage.group
  }
}

/**
 * @param {{ 'MessageBody': string }} message 
 * @returns {AggregateOfferMessage}
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
 * @returns {import('@web3-storage/filecoin-api/aggregator/api').AggregateOfferQueue}
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
