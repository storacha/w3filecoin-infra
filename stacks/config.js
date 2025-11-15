import { RemovalPolicy } from 'aws-cdk-lib'
import git from 'git-rev-sync'
import * as pack from '../package.json'

// 72 Hours
export const DEFAULT_MIN_PIECE_CRITICAL_THRESHOLD_MS = String(
  72 * 60 * 60 * 1000
)
// 60 Hours
export const DEFAULT_MIN_PIECE_WARN_THRESHOLD_MS = String(60 * 60 * 60 * 1000)
// 48 Hours
export const DEFAULT_AGGREGATE_MONITOR_THRESHOLD_MS = String(
  48 * 60 * 60 * 1000
)

/**
 * Get nicer resources name
 *
 * @param {string} name
 * @param {string} stage
 * @param {number} version
 */
export function getResourceName (name, stage, version = 0) {
  // e.g `prod-w3filecoin-cargo-database-0`
  return `${stage}-w3filecoin-${name}-${version}`
}

/**
 * Get nicer queue name
 *
 * @param {string} name
 * @param {number} version
 */
export function getQueueName (name, version = 0) {
  // e.g `prod-w3filecoin-piece-queue-0`
  return `${name}-${version}`
}

/**
 * Return the custom domain config for http api
 *
 * @param {string} stage
 * @param {string | undefined} hostedZone
 * @returns {{domainName: string, hostedZone: string} | undefined}
 */
export function getCustomDomain (stage, hostedZone) {
  // return no custom domain config if hostedZone not set
  if (!hostedZone) {
    return
  }
  /** @type Record<string,string> */
  const domainMap = { prod: hostedZone }
  const domainName = domainMap[stage] ?? `${stage}.${hostedZone}`
  return { domainName, hostedZone }
}

export function getApiPackageJson () {
  return pack
}

export function getGitInfo () {
  return {
    commit: git.long('.'),
    branch: git.branch('.')
  }
}

/**
 * Is an ephemeral build?
 *
 * @param {string} stage
 */
export function isPrBuild (stage) {
  if (!stage) throw new Error('stage must be provided')
  return stage !== 'prod' && stage !== 'staging'
}

/**
 * @param {string} name
 * @param {string} stage
 * @param {number} version
 */
export function getBucketConfig (name, stage, version = 0) {
  return {
    bucketName: getResourceName(name, stage, version),
    ...(isPrBuild(stage) && {
      autoDeleteObjects: true,
      removalPolicy: RemovalPolicy.DESTROY
    })
  }
}

/**
 * @param {import('sst/constructs').App} app
 * @param {import('sst/constructs').Stack} stack
 */
export function setupSentry (app, stack) {
  // Skip when locally
  if (app.local) {
    return
  }

  const { SENTRY_DSN } = getEnv()

  stack.addDefaultFunctionEnv({
    SENTRY_DSN
  })
}

/**
 * Get Env validating it is set.
 */
export function getEnv () {
  return {
    SENTRY_DSN: mustGetEnv('SENTRY_DSN'),
    UCAN_LOG_URL: mustGetEnv('UCAN_LOG_URL'),
    MIN_PIECE_CRITICAL_THRESHOLD_MS:
      process.env.MIN_PIECE_CRITICAL_THRESHOLD_MS ||
      DEFAULT_MIN_PIECE_CRITICAL_THRESHOLD_MS,
    MIN_PIECE_WARN_THRESHOLD_MS:
      process.env.MIN_PIECE_WARN_THRESHOLD_MS ||
      DEFAULT_MIN_PIECE_WARN_THRESHOLD_MS,
    AGGREGATE_MONITOR_THRESHOLD_MS:
      process.env.AGGREGATE_MONITOR_THRESHOLD_MS ||
      DEFAULT_AGGREGATE_MONITOR_THRESHOLD_MS,
    MONITORING_NOTIFICATIONS_ENDPOINT: mustGetEnv(
      'MONITORING_NOTIFICATIONS_ENDPOINT'
    )
  }
}

/**
 * Get Env validating it is set.
 *
 * @param {import('sst/constructs').Stack} stack
 */
export function getAggregatorEnv (stack) {
  const defaultMaxAggregateSize = String(2 ** 35)
  // testing value aligned with integration test fixtures
  const defaultMinAggregateSize =
    stack.stage === 'production' ? String(2 ** 34) : String(2 ** 12)
  const defaultMinUtilizationFactor =
    stack.stage === 'production' ? String(4) : String(10e9)

  return {
    ...getEnv(),
    AGGREGATOR_HOSTED_ZONE: process.env.AGGREGATOR_HOSTED_ZONE,
    AGGREGATOR_DID: mustGetEnv('AGGREGATOR_DID'),
    MAX_AGGREGATE_SIZE:
      process.env.MAX_AGGREGATE_SIZE || defaultMaxAggregateSize,
    MAX_AGGREGATE_PIECES: process.env.MAX_AGGREGATE_PIECES ?? '',
    MIN_AGGREGATE_SIZE:
      process.env.MIN_AGGREGATE_SIZE || defaultMinAggregateSize,
    MIN_UTILIZATION_FACTOR:
      process.env.MIN_UTILIZATION_FACTOR || defaultMinUtilizationFactor,
    AGGREGATOR_PROOF: process.env.AGGREGATOR_PROOF ?? ''
  }
}

export function getDealerEnv () {
  return {
    ...getEnv(),
    DEAL_API_HOSTED_ZONE: process.env.DEALER_API_HOSTED_ZONE,
    DEALER_DID: mustGetEnv('DEALER_DID'),
    DEALER_PROOF: process.env.DEALER_PROOF ?? ''
  }
}

export function getDealTrackerEnv () {
  return {
    ...getEnv(),
    DEAL_TRACKER_API_HOSTED_ZONE: process.env.DEAL_TRACKER_API_HOSTED_ZONE,
    DEAL_TRACKER_DID: mustGetEnv('DEAL_TRACKER_DID'),
    DEAL_TRACKER_PROOF: process.env.DEAL_TRACKER_PROOF ?? '',
    SPADE_ORACLE_URL: mustGetEnv('SPADE_ORACLE_URL')
  }
}

/**
 *
 * @param {string} name
 * @returns {string}
 */
function mustGetEnv (name) {
  const value = process.env[name]
  if (!value) throw new Error(`Missing env var: ${name}`)
  return value
}
