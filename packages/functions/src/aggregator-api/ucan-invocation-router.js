import * as Sentry from '@sentry/serverless'
import { Config } from 'sst/node/config'
import { Table } from 'sst/node/table'
// eslint-disable-next-line no-unused-vars
import * as CAR from '@ucanto/transport/car'
import * as Server from '@ucanto/server'

import { connect as ucanLogConnect } from '@w3filecoin/core/src/ucan-log.js'
// store clients
import { createClient as createAggregateStoreClient } from '@w3filecoin/core/src/store/aggregator-aggregate-store.js'
import { createClient as createBufferStoreClient } from '@w3filecoin/core/src/store/aggregator-buffer-store.js'
import { createClient as createInclusionProofStoreClient } from '@w3filecoin/core/src/store/aggregator-inclusion-proof-store.js'
import { createClient as createInclusionStoreClient } from '@w3filecoin/core/src/store/aggregator-inclusion-store.js'
import { createClient as createPieceStoreClient } from '@w3filecoin/core/src/store/aggregator-piece-store.js'
// queue clients
import { createClient as createPieceQueueClient } from '@w3filecoin/core/src/queue/piece-queue.js'
import { createClient as createBufferQueueClient } from '@w3filecoin/core/src/queue/buffer-queue.js'
import { createClient as createAggregateOfferQueueClient } from '@w3filecoin/core/src/queue/aggregate-offer-queue.js'
import { createClient as createPieceAcceptQueueClient } from '@w3filecoin/core/src/queue/piece-accept-queue.js'
// ucanto server
import { getServiceSigner, getPrincipal } from '@w3filecoin/core/src/service.js'
import { createServer } from '@web3-storage/filecoin-api/aggregator/service'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 0,
})

/**
 * AWS HTTP Gateway handler for POST / with ucan invocation router.
 *
 * We provide responses in Payload format v2.0
 * see: https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-lambda.html#http-api-develop-integrations-lambda.proxy-format
 *
 * @param {import('aws-lambda').APIGatewayProxyEventV2} request
 */
export async function ucanInvocationRouter(request) {
  if (request.body === undefined) {
    return {
      statusCode: 400,
    }
  }

  const { ucanLogUrl, ucanLogAuth } = getLambdaEnv()
  const ucanLog = ucanLogConnect({
    url: new URL(ucanLogUrl),
    auth: ucanLogAuth
  })

  const server = createServer({
    ...getContext(),
    errorReporter: {
      catch: (/** @type {string | Error} */ err) => {
        console.warn(err)
        Sentry.AWSLambda.captureException(err)
      }
    }
  })

  const payload = fromLambdaRequest(request)
  const result = server.codec.accept(payload)

  // if we can not select a codec we respond with error.
  if (result.error) {
    return toLambdaResponse({
      status: result.error.status,
      headers: result.error.headers || {},
      body: Buffer.from(result.error.message || ''),
    })
  }

  const { encoder, decoder } = result.ok
  /** @type {import('@ucanto/core').API.AgentMessage} */
  const message = await decoder.decode(payload)

  // We block until we can log the UCAN invocation if this fails we return a 500
  // to the client. That is because in the future we expect that invocations will
  // be written to a queue first and then processed asynchronously, so if we
  // fail to enqueue the invocation we should fail the request.
  await ucanLog.log(CAR.request.encode(message))

  // Execute invocations
  const outgoing = await Server.execute(message, server)
  const response = await encoder.encode(outgoing)

  // Send ucan receipt
  await ucanLog.log(CAR.response.encode(outgoing))

  return toLambdaResponse(response)
}

function getContext () {
  const {
    did,
    dealerDid,
    privateKey,
    aggregateStoreTableName,
    aggregateStoreTableRegion,
    bufferStoreBucketName,
    bufferStoreBucketRegion,
    inclusionProofStoreBucketName,
    inclusionProofStoreBucketRegion,
    inclusionStoreTableName,
    inclusionStoreTableRegion,
    pieceStoreTableName,
    pieceStoreTableRegion,
    pieceQueueUrl,
    pieceQueueRegion,
    bufferQueueUrl,
    bufferQueueRegion,
    aggregateOfferQueueUrl,
    aggregateOfferQueueRegion,
    pieceAcceptQueueUrl,
    pieceAcceptQueueRegion,
  } = getLambdaEnv()

  return {
    id: getServiceSigner({ did, privateKey }),
    dealerId: getPrincipal(dealerDid),
    aggregateStore: createAggregateStoreClient(
      { region: aggregateStoreTableRegion },
      { tableName: aggregateStoreTableName }
    ),
    bufferStore: createBufferStoreClient(
      { region: bufferStoreBucketRegion },
      { name: bufferStoreBucketName }
    ),
    inclusionStore: createInclusionStoreClient(
      { region: inclusionStoreTableRegion },
      {
        tableName: inclusionStoreTableName,
        inclusionProofStore: createInclusionProofStoreClient(
          { region: inclusionProofStoreBucketRegion },
          { name: inclusionProofStoreBucketName }
        )
      }
    ),
    pieceStore: createPieceStoreClient(
      { region: pieceStoreTableRegion },
      { tableName: pieceStoreTableName }
    ),
    pieceQueue: createPieceQueueClient(
      { region: pieceQueueRegion },
      { queueUrl: pieceQueueUrl }
    ),
    bufferQueue: createBufferQueueClient(
      { region: bufferQueueRegion },
      { queueUrl: bufferQueueUrl }
    ),
    aggregateOfferQueue: createAggregateOfferQueueClient(
      { region: aggregateOfferQueueRegion },
      { queueUrl: aggregateOfferQueueUrl }
    ),
    pieceAcceptQueue: createPieceAcceptQueueClient(
      { region: pieceAcceptQueueRegion },
      { queueUrl: pieceAcceptQueueUrl }
    ),
    // TODO: integrate with revocations
    validateAuthorization: () => ({ ok: {} })
  }
}

function getLambdaEnv () {
  const { AGGREGATOR_PRIVATE_KEY: privateKey, UCAN_LOG_BASIC_AUTH: ucanLogAuth } = Config

  return {
    did: mustGetEnv('DID'),
    dealerDid: mustGetEnv('DEALER_DID'),
    ucanLogUrl: mustGetEnv('UCAN_LOG_URL'),
    aggregateStoreTableName: Table['aggregator-aggregate-store'].tableName,
    aggregateStoreTableRegion: mustGetEnv('AWS_REGION'),
    bufferStoreBucketName: mustGetEnv('BUFFER_STORE_BUCKET_NAME'),
    bufferStoreBucketRegion: mustGetEnv('AWS_REGION'),
    inclusionProofStoreBucketName: mustGetEnv('INCLUSION_PROOF_STORE_BUCKET_NAME'),
    inclusionProofStoreBucketRegion: mustGetEnv('AWS_REGION'),
    inclusionStoreTableName: Table['aggregator-inclusion-store'].tableName,
    inclusionStoreTableRegion: mustGetEnv('AWS_REGION'),
    pieceStoreTableName: Table['aggregator-piece-store'].tableName,
    pieceStoreTableRegion: mustGetEnv('AWS_REGION'),
    pieceQueueUrl: mustGetEnv('PIECE_QUEUE_URL'),
    pieceQueueRegion: mustGetEnv('AWS_REGION'),
    bufferQueueUrl: mustGetEnv('BUFFER_QUEUE_URL'),
    bufferQueueRegion: mustGetEnv('AWS_REGION'),
    aggregateOfferQueueUrl: mustGetEnv('AGGREGATE_OFFER_QUEUE_URL'),
    aggregateOfferQueueRegion: mustGetEnv('AWS_REGION'),
    pieceAcceptQueueUrl: mustGetEnv('PIECE_ACCEPT_QUEUE_URL'),
    pieceAcceptQueueRegion: mustGetEnv('AWS_REGION'),
    privateKey,
    ucanLogAuth
  }
}

/**
 * @param {import('@ucanto/core').API.HTTPResponse} response
 */
export function toLambdaResponse({ status = 200, headers, body }) {
  return {
    statusCode: status,
    headers,
    body: Buffer.from(body).toString('base64'),
    isBase64Encoded: true,
  }
}

/**
 * @param {import('aws-lambda').APIGatewayProxyEventV2} request
 */
export const fromLambdaRequest = (request) => ({
  headers: /** @type {Record<string, string>} */ (request.headers),
  body: Buffer.from(request.body || '', 'base64'),
})

export const handler = Sentry.AWSLambda.wrapHandler(ucanInvocationRouter)
