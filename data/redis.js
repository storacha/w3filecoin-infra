import Redis from 'ioredis'

/**
 * Abstraction layer to interact with redis.
 *
 * @param {string} host
 * @param {number} port
 */
export function createRedis (host, port) {
  return new Redis({
    host,
    port,
    username: 'default', // needs Redis >= 6
    tls: {},
    connectTimeout: 2000,
  })
}
