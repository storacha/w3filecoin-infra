import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs'

const queueUrl = 'https://sqs.us-west-2.amazonaws.com/505595374361/vcs-w3filecoin-grouping-queue-0.fifo'
const region = 'us-west-2'

const client = new SQSClient({
  region
})

const messageData = [
  {
    body: '1',
    groupId: '1'
  },
  {
    body: '2',
    groupId: '1'
  },
  {
    body: '3',
    groupId: '1'
  },
  {
    body: '4',
    groupId: '1'
  },
  {
    body: '5',
    groupId: '2'
  },
  {
    body: '6',
    groupId: '2'
  },
  {
    body: '7',
    groupId: '2'
  },
  {
    body: '8',
    groupId: '2'
  }
]

const queueResponses = await Promise.all(
  messageData.map(m => client.send(new SendMessageCommand({
    QueueUrl: queueUrl,
    MessageBody: m.body,
    MessageGroupId: m.groupId
  })))
)

console.log('queued responses', queueResponses)
