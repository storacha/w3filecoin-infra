import { SendMessageCommand } from '@aws-sdk/client-sqs'

import { connectQueue } from './index.js'

/**
 * @template T
 * @param {import('./types.js').QueueConnect | import('@aws-sdk/client-sqs').SQSClient} conf
 * @param {object} context
 * @param {string} context.queueUrl
 * @param {(item: T) => string} context.encodeMessage
 * @returns {import('@web3-storage/filecoin-api/types').Queue<T>}
 */
export function createQueueClient (conf, context) {
  const queueClient = connectQueue(conf)
  return {
    add: async (record) => {
      let encodedRecord
      try {
        encodedRecord = context.encodeMessage(record)
      } catch (/** @type {any} */ err) {
        return {
          // TODO: specify error
          error: err
        }
      }

      const cmd = new SendMessageCommand({
        QueueUrl: context.queueUrl,
        MessageBody: encodedRecord
      })

      try {
        await queueClient.send(cmd)
      } catch (/** @type {any} */ err) {
        return {
          // TODO: specify error
          error: err
        }
      }

      return {
        ok: {}
      }
    }
  }
}
