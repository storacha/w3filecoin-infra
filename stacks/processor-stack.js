import {
  setupSentry
} from './config.js'

/**
 * @param {import('sst/constructs').StackContext} properties
 */
export function ProcessorStack({ stack, app }) {
  // Setup app monitoring with Sentry
  setupSentry(app, stack)
}
