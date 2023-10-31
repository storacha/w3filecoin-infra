import { encode as JSONencode, decode as JSONdecode } from '@ipld/dag-json'
import { toString } from 'uint8arrays/to-string'
import { fromString } from 'uint8arrays/from-string'

import { createQueueClient } from './client.js'

/**
 * @typedef {import('@web3-storage/filecoin-api/aggregator/api').PieceAcceptMessage} PieceAcceptMessage
 * @typedef {import('./client.js').ClientEncodedMessage} ClientEncodedMessage
 */

/**
 * @param {PieceAcceptMessage} pieceAcceptMessage
 * @returns {ClientEncodedMessage}
 */
const encodeMessage = (pieceAcceptMessage) => {
  const encodedBytes = JSONencode(pieceAcceptMessage)
  return {
    MessageBody: toString(encodedBytes)
  }
}

/**
 * @param {{ 'MessageBody': string }} message 
 * @returns {PieceAcceptMessage}
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
 * @returns {import('@web3-storage/filecoin-api/aggregator/api').PieceAcceptQueue}
 */
export function createClient (conf, context) {
  return createQueueClient(conf,
    {
      queueUrl: context.queueUrl,
      encodeMessage
    })
}
