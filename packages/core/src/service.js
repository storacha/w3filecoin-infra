import * as ed25519 from '@ucanto/principal/ed25519'
import * as Server from '@ucanto/server'
import * as CAR from '@ucanto/transport/car'
import * as DID from '@ipld/dag-ucan/did'

import { createService } from '@web3-storage/filecoin-api/aggregator'

/**
 * @param {import('@ucanto/interface').Signer} servicePrincipal
 * @param {import('@web3-storage/filecoin-api/types').AggregatorServiceContext} context
 * @param {import('./types').ErrorReporter} errorReporter
 */
export const createUcantoServer = (servicePrincipal, context, errorReporter) =>
  Server.create({
    id: servicePrincipal,
    codec: CAR.inbound,
    service: createService(context),
    catch: (error) => errorReporter.catch(error),
  })

/**
 * Given a config, return a ucanto Signer object representing the service
 *
 * @param {object} config
 * @param {string} config.privateKey - multiformats private key of primary signing key
 * @param {string} [config.did] - public DID for the service (did:key:... derived from PRIVATE_KEY if not set)
 * @returns {import('@ucanto/principal/ed25519').Signer.Signer}
 */
 export function getServiceSigner(config) {
  const signer = ed25519.parse(config.privateKey)
  if (config.did) {
    const did = DID.parse(config.did).did()
    return signer.withDID(did)
  }
  return signer
}
