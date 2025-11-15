import * as Signer from '@ucanto/principal/ed25519'
import * as DID from '@ipld/dag-ucan/did'
import { CAR, HTTP } from '@ucanto/transport'
import { connect } from '@ucanto/client'

/**
 * Client connection config for w3filecoin pipeline entrypoint, i.e. API Storefront relies on
 *
 * @param {URL} url
 */
export async function getClientConfig (url) {
  // UCAN actors
  const storefront = getServiceSigner({
    did: 'did:web:staging.web3.storage',
    privateKey: process.env.AGGREGATOR_PRIVATE_KEY || ''
  })
  const aggregatorService = DID.parse('did:web:staging.web3.storage')

  return {
    invocationConfig: {
      issuer: storefront,
      with: storefront.did(),
      audience: aggregatorService
    },
    connection: connect({
      id: aggregatorService,
      codec: CAR.outbound,
      channel: HTTP.open({
        url,
        method: 'POST'
      })
    })
  }
}

/**
 * Given a config, return a ucanto Signer object representing the service
 *
 * @param {object} config
 * @param {string} config.privateKey - multiformats private key of primary signing key
 * @param {string} [config.did] - public DID for the service (did:key:... derived from PRIVATE_KEY if not set)
 * @returns {import('@ucanto/principal/ed25519').Signer.Signer}
 */
export function getServiceSigner (config) {
  const signer = Signer.parse(config.privateKey)
  if (config.did) {
    const did = DID.parse(config.did).did()
    return signer.withDID(did)
  }
  return signer
}
