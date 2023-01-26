import * as Sentry from '@sentry/serverless'
import Redis from 'ioredis'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 1.0,
})

/**
 * AWS HTTP Gateway handler for GET /
 */

export async function redisGet () {
  const { REDIS_HOST: host, REDIS_KEY: key } = getEnv()

  const redis = new Redis({
    port: 6379,
    host,
    username: 'default', // needs Redis >= 6
    tls: {},
    connectTimeout: 2000,
  })

  // Makes sure one value exists if none exists
  const redisOp = await redis
    .multi()
    .setnx(key, Date.now())
    .get(key)
    .exec()

  const response = {
    [key]: redisOp?.[1][1] // Redis get result
  }

  return {
    statusCode: 200,
    headers: {
      'Content-Type': 'application/json; charset=utf-8'
    },
    body: JSON.stringify(response)
  }
}

export const get = Sentry.AWSLambda.wrapHandler(redisGet)

/**
 * Get Env validating it is set.
 */
function getEnv() {
  return {
    REDIS_HOST: mustGetEnv('REDIS_HOST'),
    REDIS_KEY: mustGetEnv('REDIS_KEY'),
  }
}

/**
 * @param {string} name 
 * @returns {string}
 */
function mustGetEnv (name) {
  if (!process.env[name]) {
    throw new Error(`Missing env var: ${name}`)
  }

  // @ts-expect-error there will always be a string there, but typescript does not believe
  return process.env[name]
}
