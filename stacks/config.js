import git from 'git-rev-sync'
import * as pack from '../package.json'

export const DEFAULT_FERRY_CARGO_MAX_SIZE = 127*(1<<28)
export const DEFAULT_FERRY_CARGO_MIN_SIZE = 1+127*(1<<27)

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
    SENTRY_DSN,
  })
}

/**
 * Get Env validating it is set.
 */
export function getEnv() {
  return {
    SENTRY_DSN: mustGetEnv('SENTRY_DSN'),
    CONTENT_RESOLVER_URL_R2: mustGetEnv('CONTENT_RESOLVER_URL_R2')
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
