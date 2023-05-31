import { connect } from '@ucanto/client'
import { CAR, HTTP } from '@ucanto/transport'
import * as DID from '@ipld/dag-ucan/did'

/**
 * @param {string} name 
 * @returns {string}
 */
export function mustGetEnv (name) {
  const value = process.env[name]
  if (!value) throw new Error(`Missing env var: ${name}`)
  return value
}

/**
 * @param {import('../types').AggregationServiceCtx} config 
 */
export function getAggregationServiceConnection(config) {
  const aggregationServicePrincipal = DID.parse(config.DID) // 'did:web:spade.web3.storage'
  const aggregationServiceURL = new URL(config.URL) // 'https://spade-proxy.web3.storage'

  const aggregationServiceConnection = connect({
    id: aggregationServicePrincipal,
    codec: CAR.outbound,
    channel: HTTP.open({
      url: aggregationServiceURL,
      method: 'POST',
    }),
  })

  return aggregationServiceConnection
}
