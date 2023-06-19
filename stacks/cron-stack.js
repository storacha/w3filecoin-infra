import { Cron, use } from 'sst/constructs'

import { DbStack } from './db-stack.js'
import {
  setupSentry,
  getResourceName
} from './config.js'

/**
 * @param {import('sst/constructs').StackContext} properties
 */
export function CronStack({ stack, app }) {
  // Setup app monitoring with Sentry
  setupSentry(app, stack)

  const { db } = use(DbStack)

  const loadFerryCronName = getResourceName('arrange-offers-cron', stack.stage)
  new Cron(stack, loadFerryCronName, {
    schedule: 'rate(30 minutes)',
    job: {
      function: {
        handler: 'packages/functions/src/cron/load-ferry.main',
        bind: [
          db
        ]
      },
    }
  })
}
