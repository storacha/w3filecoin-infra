import { createRequire } from 'module'
import fs from 'fs'
import path from 'path'

import { S3Client } from '@aws-sdk/client-s3'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'

export function getStage () {
  const stage = process.env.SST_STAGE || process.env.SEED_STAGE_NAME
  if (stage) {
    return stage
  }

  const f = fs.readFileSync(path.join(
    process.cwd(),
    '.sst/stage'
  ))

  return f.toString()
}

export const getStackName = () => {
  const stage = getStage()
  return `${stage}-w3filecoin`
}

export const getAwsRegion = () => {
  // CI/CD deployment
  if (process.env.SEED_APP_NAME) {
    return 'us-east-2'
  }

  return 'us-west-2'
}

export const getBufferStoreBucketInfo = () => {
  const stage = getStage()
  const region = getAwsRegion()
  const client = new S3Client({
    region
  })

  // CI/CD deployment
  if (process.env.SEED_APP_NAME) {
    return {
      client,
      bucket: `buffer-store-${stage}-0`,
      region
    }
  }

  const require = createRequire(import.meta.url)
  const testEnv = require(path.join(
    process.cwd(),
    '.sst/outputs.json'
  ))

  // Get bucket Name
  const id = 'DataStack'
  return {
    client,
    bucket: /** @type {string} */ (testEnv[`${getStackName()}-${id}`].BufferBucketName),
    region
  }
}

export const getPieceStoreDynamoDb = () => {
  // CI/CD deployment
  if (process.env.SEED_APP_NAME) {
    return getDynamoDb('piece-store')
  }

  const require = createRequire(import.meta.url)
  const testEnv = require(path.join(
    process.cwd(),
    '.sst/outputs.json'
  ))

  // Get Table Name
  const id = 'DataStack'
  const tableName = testEnv[`${getStackName()}-${id}`].PieceTableName

  return getDynamoDb(tableName)
}

export const getAggregateStoreDynamoDb = () => {
  // CI/CD deployment
  if (process.env.SEED_APP_NAME) {
    return getDynamoDb('aggregate-store')
  }

  const require = createRequire(import.meta.url)
  const testEnv = require(path.join(
    process.cwd(),
    '.sst/outputs.json'
  ))

  // Get Table Name
  const id = 'DataStack'
  const tableName = testEnv[`${getStackName()}-${id}`].AggregateTableName

  return getDynamoDb(tableName)
}

/**
 * @param {string} tableName 
 */
export const getDynamoDb = (tableName) => {
  const region = getAwsRegion()
  const endpoint = `https://dynamodb.${region}.amazonaws.com`

  return {
    client: new DynamoDBClient({
      region,
      endpoint
    }),
    tableName: `${getStackName()}-${tableName}`,
    region,
    endpoint
  }
}

export const getApiEndpoint = () => {
  const stage = getStage()

  // CI/CD deployment
  if (process.env.SEED_APP_NAME) {
    return `https://${stage}.filecoin.web3.storage`
  }

  const require = createRequire(import.meta.url)
  const testEnv = require(path.join(
    process.cwd(),
    '.sst/outputs.json'
  ))

  // Get Upload API endpoint
  const id = 'ApiStack'
  return testEnv[`${getStackName()}-${id}`].ApiEndpoint
}
