import { Config } from 'sst/node/config'
import * as Sentry from '@sentry/serverless'
import { getServiceSigner } from '@w3filecoin/core/src/service.js'

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
  const { DEALER_PRIVATE_KEY: privateKey } = Config
  const { did, name, version, commit, stage } = getLambdaEnv()
  const serviceSigner = getServiceSigner({ did, privateKey })
  const serviceDid = serviceSigner.did()
  const publicKey = serviceSigner.toDIDKey()

  return {
    statusCode: 200,
    headers: {
      'Content-Type': `application/json`
    },
    body: JSON.stringify({ name, version, did: serviceDid, publicKey, repo, commit, env: stage })
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
    did: mustGetEnv('DID'),
    name: mustGetEnv('NAME'),
    version: mustGetEnv('VERSION'),
    commit: mustGetEnv('COMMIT'),
    stage: mustGetEnv('STAGE'),
  }
}
