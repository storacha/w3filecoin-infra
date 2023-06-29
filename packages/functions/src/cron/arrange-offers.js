import * as Sentry from '@sentry/serverless'
import { RDS } from 'sst/node/rds'

import { createCargoTable } from '@w3filecoin/core/src/table/cargo.js'
import { createFerryTable } from '@w3filecoin/core/src/table/ferry.js'
import { arrangeOffers } from '@w3filecoin/core/src/arrange-offers.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

async function handler() {
  // @ts-ignore
  console.log('RRR', RDS.db.secretArn)
  // @ts-expect-error rds.Cluster has no types...
  const { defaultDatabaseName, secretArn, clusterArn} = RDS.Cluster

  const cargoTable = createCargoTable({
    database: defaultDatabaseName,
    secretArn: secretArn,
    resourceArn: clusterArn,
  })

  const ferryTable = createFerryTable({
    database: defaultDatabaseName,
    secretArn: secretArn,
    resourceArn: clusterArn,
  })

  await arrangeOffers(cargoTable, ferryTable)

  return {
    statusCode: 200
  }
}

export const main = Sentry.AWSLambda.wrapHandler(handler)
