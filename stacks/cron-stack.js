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

  // CRON responsible for attempting to load ferries if there is enough cargo queued
  const loadFerryCronName = getResourceName('load-ferry-cron', stack.stage)
  new Cron(stack, loadFerryCronName, {
    schedule: 'rate(15 minutes)',
    job: {
      function: {
        handler: 'packages/functions/src/cron/load-ferry.main',
        bind: [
          db
        ]
      },
    }
  })

  // CRON responsible for attempting to offer queued aggregates offer
  const aggregateOfferCronName = getResourceName('aggregate-offer-cron', stack.stage)
  new Cron(stack, aggregateOfferCronName, {
    schedule: 'rate(15 minutes)',
    job: {
      function: {
        handler: 'packages/functions/src/cron/aggregate-offer.main',
        bind: [
          db
        ]
      },
    }
  })

  // CRON responsible
  const arrangeOfferCronName = getResourceName('arrange-offers-cron', stack.stage)
  new Cron(stack, arrangeOfferCronName, {
    schedule: 'rate(15 minutes)',
    job: {
      function: {
        handler: 'packages/functions/src/cron/arrange-offers.main',
        bind: [
          db
        ]
      },
    }
  })
}
