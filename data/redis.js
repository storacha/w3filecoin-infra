import Redis from 'ioredis'

/**
 * Abstraction layer to interact with redis.
 *
 * @param {string} host
 * @param {number} port
 * @param {any} [opts]
 */
export function createRedis (host, port, opts = {}) {
  return new Redis({
    host,
    port,
    username: 'default', // needs Redis >= 6
    connectTimeout: 2000,
    ...opts
  })
}
