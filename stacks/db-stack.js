import { RDS } from 'sst/constructs'

import {
  setupSentry
} from './config.js'

/**
 * @param {import('sst/constructs').StackContext} properties
 */
export function DbStack({ stack, app }) {
  // Setup app monitoring with Sentry
  setupSentry(app, stack)

  const dbName = 'w3filecoinrds'
  const db = new RDS(stack, dbName, {
    engine: 'postgresql11.13',
    defaultDatabaseName: dbName,
    migrations: 'packages/core/migrations',
    types: 'packages/core/src/schema.ts',
    // https://docs.sst.dev/constructs/RDS#auto-scaling
    scaling: stack.stage !== 'production' ?
      {
        autoPause: true,
        minCapacity: 'ACU_2',
        maxCapacity: 'ACU_2',
      }
      :
      {
        autoPause: false,
        minCapacity: 'ACU_4',
        maxCapacity: 'ACU_64',
      }
  })

  return {
    db
  }
}
