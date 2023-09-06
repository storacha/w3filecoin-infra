import * as Sentry from '@sentry/serverless'
import { Config } from 'sst/node/config'
import { Table } from 'sst/node/table'
// eslint-disable-next-line no-unused-vars
import { API } from '@ucanto/core'
import * as CAR from '@ucanto/transport/car'
import * as Server from '@ucanto/server'

import { connect as ucanLogConnect } from '@w3filecoin/core/src/ucan-log.js'
import { createTableStoreClient } from '@w3filecoin/core/src/store/table-client.js'
import { createQueueClient } from '@w3filecoin/core/src/queue/client.js'
import { createUcantoServer, getServiceSigner } from '@w3filecoin/core/src/service.js'
import { encode, decode } from '@w3filecoin/core/src/data/piece.js'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
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
  const {
    did,
    ucanLogUrl,
    pieceStoreTableName,
    pieceStoreTableRegion,
    pieceAddQueueUrl,
    pieceAddQueueRegion,
  } = getLambdaEnv()

  if (request.body === undefined) {
    return {
      statusCode: 400,
    }
  }

  const { PRIVATE_KEY: privateKey, UCAN_LOG_BASIC_AUTH } = Config

  const ucanLog = ucanLogConnect({
    url: new URL(ucanLogUrl),
    auth: UCAN_LOG_BASIC_AUTH
  })

  // Context
  const serviceSigner = getServiceSigner({ did, privateKey })
  const pieceStore = createTableStoreClient({
    region: pieceStoreTableRegion
  }, {
    tableName: pieceStoreTableName.tableName,
    encodeRecord: encode.storeRecord,
    decodeRecord: decode.storeRecord,
    encodeKey: encode.storeKey
  })
  const addQueue = createQueueClient({
    region: pieceAddQueueRegion
  }, {
    queueUrl: pieceAddQueueUrl,
    encodeMessage: encode.message,
  })

  const server = createUcantoServer(serviceSigner, {
    pieceStore,
    addQueue,
    id: serviceSigner
  }, {
    catch: (/** @type {string | Error} */ err) => {
      console.warn(err)
      Sentry.AWSLambda.captureException(err)
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
  /** @type {API.AgentMessage} */
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

function getLambdaEnv () {
  return {
    did: mustGetEnv('DID'),
    dealerDid: mustGetEnv('DEALER_DID'),
    dealerUrl: mustGetEnv('DEALER_URL'),
    ucanLogUrl: mustGetEnv('UCAN_LOG_URL'),
    pieceStoreTableName: Table['piece-store'],
    pieceStoreTableRegion: mustGetEnv('AWS_REGION'),
    pieceAddQueueUrl: mustGetEnv('PIECE_ADD_QUEUE_URL'),
    pieceAddQueueRegion: mustGetEnv('PIECE_ADD_QUEUE_REGION')
  }
}

/**
 * @param {API.HTTPResponse} response
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
