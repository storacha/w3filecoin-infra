import * as Sentry from '@sentry/serverless'

import parseSqsEvent from '../utils/parse-sqs-event.js'

// import { createCarTable } from '../tables/car.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * Get EventRecord from the SQS Event triggering the handler
 *
 * @param {import('aws-lambda').SQSEvent} event
 */
function carMetadataWriterHandler (event) {
  const {
    CAR_TABLE_NAME,
  } = getEnv()

  // TODO: multiple
  const record = parseSqsEvent(event)
  if (!record) {
    throw new Error('Invalid CAR file format')
  }

  throw new Error(`NOT_YET_IMPLEMENTED: ${CAR_TABLE_NAME}`)
}

export const handler = Sentry.AWSLambda.wrapHandler(carMetadataWriterHandler)

/**
 * Get Env validating it is set.
 */
function getEnv() {
  return {
    CAR_TABLE_NAME: mustGetEnv('CAR_TABLE_NAME')
  }
}

/**
 * 
 * @param {string} name 
 * @returns {string}
 */
function mustGetEnv (name) {
  if (!process.env[name]) {
    throw new Error(`Missing env var: ${name}`)
  }

  // @ts-expect-error there will always be a string there, but typescript does not believe
  return process.env[name]
}
