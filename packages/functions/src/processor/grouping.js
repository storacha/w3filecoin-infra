import * as Sentry from '@sentry/serverless'

/**
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 */
async function groupingWorkflow (sqsEvent) {
  if (sqsEvent.Records.length === 1) {
    return {
      batchItemFailures: sqsEvent.Records.map(r => r.messageId)
    }
  }
  console.log('a grouping batch', sqsEvent.Records.map((r) => ({
    body: r.body,
    id: r.messageId,
    messageGroupId: r.attributes.MessageGroupId
  })))
}

export const workflow = Sentry.AWSLambda.wrapHandler(groupingWorkflow)
