import { RDS } from 'sst/constructs'

import {
  setupSentry,
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
    types: 'packages/core/src/sql.generated.ts'
  })

  return {
    db
  }
}
