import { GenericContainer as Container } from 'testcontainers'
import pRetry from 'p-retry'
import { customAlphabet } from 'nanoid'
import { S3Client, CreateBucketCommand } from '@aws-sdk/client-s3'
import { SQSClient, CreateQueueCommand } from '@aws-sdk/client-sqs'
import { DynamoDBClient, CreateTableCommand } from '@aws-sdk/client-dynamodb'

/**
 * @param {object} [opts]
 * @param {number} [opts.port]
 * @param {string} [opts.region]
 */
export async function createDynamodDb(opts = {}) {
  const port = opts.port || 8000
  const region = opts.region || 'us-west-2'
  const dbContainer = await pRetry(() =>
    new Container('amazon/dynamodb-local:latest')
      .withExposedPorts(port)
      .start()
  )

  const endpoint = `http://${dbContainer.getHost()}:${dbContainer.getMappedPort(8000)}`
  return {
    client: new DynamoDBClient({
      region,
      endpoint
    }),
    endpoint
  }
}

/**
 * @param {import("@aws-sdk/client-dynamodb").DynamoDBClient} dynamo
 * @param {import('sst/constructs').TableProps} props
 */
export async function createTable(dynamo, props) {
  const id = customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10)
  const tableName = id()

  await dynamo.send(
    new CreateTableCommand({
      TableName: tableName,
      ...dynamoDBTableConfig(props),
      ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1,
      },
    })
  )

  return tableName
}

/**
 * Convert SST TableProps to DynamoDB `CreateTableCommandInput` config
 * 
 * @typedef {import('@aws-sdk/client-dynamodb').CreateTableCommandInput} CreateTableCommandInput
 * @typedef {import('sst/constructs').TableProps} TableProps
 *
 * @param {TableProps} props
 * @returns {Pick<CreateTableCommandInput, 'AttributeDefinitions' | 'KeySchema' | 'GlobalSecondaryIndexes'>}
 */
export function dynamoDBTableConfig ({ fields, primaryIndex, globalIndexes = {} }) {
  if (!primaryIndex || !fields) throw new Error('Expected primaryIndex and fields on TableProps')
  const globalIndexValues = Object.values(globalIndexes)
  const attributes = [
    ...Object.values(primaryIndex),
    ...globalIndexValues.map((value) => value.partitionKey),
    ...globalIndexValues.map((value) => value.sortKey)
  ]

  const AttributeDefinitions = Object.entries(fields)
    .filter(([k]) => attributes.includes(k)) // 'The number of attributes in key schema must match the number of attributes defined in attribute definitions'
    .map(([k, v]) => ({
      AttributeName: k,
      AttributeType: v[0].toUpperCase()
    }))
  const KeySchema = toKeySchema(primaryIndex)
  const GlobalSecondaryIndexes = Object.entries(globalIndexes)
    .map(([IndexName, val]) => ({
      IndexName,
      KeySchema: toKeySchema(val),
      Projection: { ProjectionType: 'ALL' },
      ProvisionedThroughput: {
        ReadCapacityUnits: 5,
        WriteCapacityUnits: 5
      }
    }))

  return {
    AttributeDefinitions,
    KeySchema,
    GlobalSecondaryIndexes: GlobalSecondaryIndexes.length ? GlobalSecondaryIndexes : undefined
  }
}

/**
 * @param {object} index
 * @param {string} index.partitionKey
 * @param {string} [index.sortKey]
 */
function toKeySchema ({partitionKey, sortKey}) {
  const KeySchema = [
    { AttributeName: partitionKey, KeyType: 'HASH' }
  ]
  if (sortKey) {
    KeySchema.push(
      { AttributeName: sortKey, KeyType: 'RANGE' }
    )
  }
  return KeySchema
}

/**
 * @param {object} [opts]
 * @param {number} [opts.port]
 * @param {string} [opts.region]
 */
export async function createS3(opts = {}) {
  const region = opts.region || 'us-west-2'
  const port = opts.port || 9000

  const minio = await pRetry(() =>
    new Container('quay.io/minio/minio')
      .withCommand(['server', '/data'])
      .withExposedPorts(port)
      .start()
  )

  const clientOpts = {
    endpoint: `http://${minio.getHost()}:${minio.getMappedPort(port)}`,
    forcePathStyle: true,
    region,
    credentials: {
      accessKeyId: 'minioadmin',
      secretAccessKey: 'minioadmin',
    },
  }

  return {
    client: new S3Client(clientOpts),
    clientOpts,
  }
}

/**
 * @param {S3Client} s3
 */
export async function createBucket(s3) {
  const id = customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10)
  const Bucket = id()
  await pRetry(() => s3.send(new CreateBucketCommand({ Bucket })))
  return Bucket
}

/**
 * @param {object} [opts]
 * @param {number} [opts.port]
 * @param {string} [opts.region]
 */
export async function createQueue(opts = {}) {
  const region = opts.region || 'us-west-2'
  const port = opts.port || 9324

  const queue = await pRetry(() =>
    new Container('softwaremill/elasticmq')
      .withExposedPorts(port)
      .start()
  )

  const endpoint = `http://${queue.getHost()}:${queue.getMappedPort(port)}`
  const client = new SQSClient({
    region,
    endpoint
  })
  const accountId = '000000000000'
  const id = customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10)
  const QueueName = id()

  await pRetry(() =>
    client.send(new CreateQueueCommand({
      QueueName,
    }))
  )

  return {
    client,
    queueName: QueueName,
    queueUrl: `${endpoint}/${accountId}/${QueueName}`
  }
}
