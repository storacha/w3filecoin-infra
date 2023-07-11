import { promises as fs } from 'fs'
import path from 'path'
import { GenericContainer as Container, PostgreSqlContainer } from 'testcontainers'
import { customAlphabet } from 'nanoid'
import { S3Client, CreateBucketCommand } from '@aws-sdk/client-s3'
import Pool from 'pg-pool'
import { Kysely, PostgresDialect, Migrator, FileMigrationProvider } from 'kysely'
import { SQSClient, CreateQueueCommand } from '@aws-sdk/client-sqs'

export async function createDatabase () {
  const database = (customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10))()
  const container = await new PostgreSqlContainer().withDatabase(database).start()

  const client = new Kysely({
    dialect: new PostgresDialect({
      pool: new Pool({
        host: container.getHost(),
        port: container.getPort(),
        database: container.getDatabase(),
        user: container.getUsername(),
        password: container.getPassword()
      })
    })
  })

  // Perform migrations
  const migrator = new Migrator({
    db: client,
    provider: new FileMigrationProvider({
      fs,
      path,
      // Path to the folder that contains all your migrations.
      migrationFolder: `${process.cwd()}/migrations`
    })
  })

  await migrator.migrateToLatest()

  return {
    client
  }
}

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
