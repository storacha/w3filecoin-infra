import * as Sentry from '@sentry/serverless'

import { createPieceQueue } from '@w3filecoin/core/src/queue/piece'
import { createAggregateQueue } from '@w3filecoin/core/src/queue/aggregate'
import * as aggregatorWorkflow from '@w3filecoin/core/src/workflow/aggregator'

import { getDbEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * Reads queued pieces and adds it to the aggregate queue
 */
async function handler() {
  const db = getDbEnv()
  const pieceQueue = createPieceQueue(db)
  const aggregateQueue = createAggregateQueue(db)

  const { ok, error } = await aggregatorWorkflow.run({ pieceQueue, aggregateQueue })
  if (error) {
    return {
      statusCode: 500,
      body: error.name
    }
  }

  return {
    statusCode: 200,
    body: ok?.count
  }
}

export const run = Sentry.AWSLambda.wrapHandler(handler)
