import * as Sentry from '@sentry/serverless'
import { RDS } from '@serverless-stack/node/rds'

// import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

// const AWS_REGION = mustGetEnv('AWS_REGION')

/**
 * @param {import('aws-lambda').DynamoDBStreamEvent} event
 */
async function handler(event) {
  console.log('event', event)
  // @ts-ignore
  console.log('RRR', RDS.db.secretArn)
}

export const main = Sentry.AWSLambda.wrapHandler(handler)
