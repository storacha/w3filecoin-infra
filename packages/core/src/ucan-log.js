import pRetry from 'p-retry'

/**
 *
 * @typedef {object} UCANLogProps
 * @property {URL} url
 * @property {string} [auth]
 */

/**
 * @param {UCANLogProps} input
 */
export const connect = (input) => new UCANLog(input)

class UCANLog {
  /**
   * @param {UCANLogProps} props
   */
  constructor ({ url, auth }) {
    this.url = url
    this.auth = auth
  }

  /**
   *
   * @param {import('@ucanto/interface').HTTPRequest} request
   */
  async log (request) {
    try {
      await pRetry(
        async () => {
          const res = await fetch(`${this.url}`, {
            method: 'POST',
            headers: {
              ...request.headers,
              Authorization: `Basic ${this.auth}`
            },
            body: request.body
          })

          if (!res.ok) {
            const reason = await res.text().catch(() => '')
            throw new Error(`HTTP post failed: ${res.status} - ${reason}`)
          }
        },
        {
          retries: 3
        }
      )
    } catch (error) {
      throw new Error(`Failed to log agent message: ${error}`, { cause: error })
    }
  }
}
