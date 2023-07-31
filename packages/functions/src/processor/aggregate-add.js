import * as Sentry from '@sentry/serverless'
import { Bucket } from 'sst/node/bucket'
import { Table } from 'sst/node/table'

import { createTableStoreClient } from '@w3filecoin/core/src/store/client/table.js'
import { createBucketStoreClient } from '@w3filecoin/core/src/store/client/bucket.js'
import { addAggregate } from '@w3filecoin/core/src/workflow/aggregate-add.js'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * Get EventRecord from the SQS Event triggering the handler.
 * The event contains a batch of `aggregate`(s) provided from `buffer-queue`. These
 * aggregates should be stored and added to the broker.
 *
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 */
async function aggregateAddWorkflow (sqsEvent) {
  if (sqsEvent.Records.length !== 1) {
    return {
      statusCode: 400,
      body: `Expected 1 sqsEvent per invocation but received ${sqsEvent.Records.length}`
    }
  }

  const { bufferStoreClient, aggregateStoreClient } = getProps()
  const aggregateRecord = sqsEvent.Records[0].body
  const groupId = sqsEvent.Records[0].attributes.MessageGroupId

  await addAggregate({
    bufferStoreClient,
    aggregateStoreClient,
    aggregateRecord,
    groupId
  })

  return {
    statusCode: 200,
    body: sqsEvent.Records.length
  }
}

/**
 * Get props clients
 */
function getProps () {
  const { bufferStoreBucketName, bufferStoreBucketRegion, aggregateStoreTableName, aggregateStoreTableRegion } = getEnv()

  return {
    bufferStoreClient: createBucketStoreClient({
      name: bufferStoreBucketName.bucketName,
      region: bufferStoreBucketRegion
    }),
    aggregateStoreClient: createTableStoreClient({
      name: aggregateStoreTableName.tableName,
      region: aggregateStoreTableRegion
    })
  }
}

/**
 * Get Env validating it is set.
 */
function getEnv () {
  return {
    bufferStoreBucketName: Bucket['buffer-store'],
    bufferStoreBucketRegion: mustGetEnv('AWS_REGION'),
    aggregateStoreTableName: Table['aggregate-store'],
    aggregateStoreTableRegion: mustGetEnv('AWS_REGION'),
    brokerDid: mustGetEnv('BROKER_DID'),
  }
}

export const workflow = Sentry.AWSLambda.wrapHandler(aggregateAddWorkflow)
