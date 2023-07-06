import * as Sentry from '@sentry/serverless'

import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

const repo = 'https://github.com/web3-storage/w3filecoin'

/**
 * AWS HTTP Gateway handler for GET /version
 */
export async function versionGet () {
  const { name , version, commit, stage } = getLambdaEnv()
  return {
    statusCode: 200,
    headers: {
      'Content-Type': `application/json`
    },
    body: JSON.stringify({ name, version, repo, commit, env: stage })
  }
}

export const version = Sentry.AWSLambda.wrapHandler(versionGet)

/**
 * AWS HTTP Gateway handler for GET /
 */
export async function homeGet () {
  const { version, stage } = getLambdaEnv()
  const env = stage === 'prod' ? '' : `(${stage})`
  return {
    statusCode: 200,
    headers: {
      'Content-Type': 'text/plain; charset=utf-8'
    },
    body: `⁂ w3filecoin-api v${version} ${env}\n- ${repo}`
  }
}

export const home = Sentry.AWSLambda.wrapHandler(homeGet)

/**
 * AWS HTTP Gateway handler for GET /error
 */
 export async function errorGet () {
  throw new Error('API Error')
}

export const error = Sentry.AWSLambda.wrapHandler(errorGet)

function getLambdaEnv () {
  return {
    name: mustGetEnv('NAME'),
    version: mustGetEnv('VERSION'),
    commit: mustGetEnv('COMMIT'),
    stage: mustGetEnv('STAGE'),
  }
}
