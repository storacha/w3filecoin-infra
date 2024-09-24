import { SQSClient } from '@aws-sdk/client-sqs'

/** @type {Record<string, import('@aws-sdk/client-sqs').SQSClient>} */
const sqsClients = {}

/**
 * @param {import('./types.js').QueueConnect | SQSClient} target 
 */
export function connectQueue (target) {
  if (target instanceof SQSClient) {
    return target
  }
  if (!sqsClients[target.region]) {
    sqsClients[target.region] = new SQSClient(target)
  }
  return sqsClients[target.region]
}
