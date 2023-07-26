import { GenericContainer as Container } from 'testcontainers'
import { customAlphabet } from 'nanoid'
import { S3Client, CreateBucketCommand } from '@aws-sdk/client-s3'
import { SQSClient, CreateQueueCommand } from '@aws-sdk/client-sqs'

/**
 * @param {object} [opts]
 * @param {number} [opts.port]
 * @param {string} [opts.region]
 */
export async function createBucket(opts = {}) {
  const region = opts.region || 'us-west-2'
  const port = opts.port || 9000

  const minio = await new Container('quay.io/minio/minio')
    .withCommand(['server', '/data'])
    .withExposedPorts(port)
    .start()

  const clientOpts = {
    endpoint: `http://${minio.getHost()}:${minio.getMappedPort(port)}`,
    forcePathStyle: true,
    region,
    credentials: {
      accessKeyId: 'minioadmin',
      secretAccessKey: 'minioadmin',
    },
  }

  const client = new S3Client(clientOpts)
  const id = customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10)
  const Bucket = id()
  await client.send(new CreateBucketCommand({ Bucket }))

  return {
    client,
    clientOpts,
    bucketName: Bucket
  }
}

/**
 * @param {object} [opts]
 * @param {number} [opts.port]
 * @param {string} [opts.region]
 */
export async function createQueue(opts = {}) {
  const region = opts.region || 'us-west-2'
  const port = opts.port || 9324

  const queue = await new Container('softwaremill/elasticmq')
    .withExposedPorts(port)
    .start()

  const endpoint = `http://${queue.getHost()}:${queue.getMappedPort(port)}`
  const client = new SQSClient({
    region,
    endpoint
  })
  const accountId = '000000000000'
  const id = customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10)
  const QueueName = id()

  await client.send(new CreateQueueCommand({
    QueueName,
  }))

  return {
    client,
    queueName: QueueName,
    queueUrl: `${endpoint}/${accountId}/${QueueName}`
  }
}
