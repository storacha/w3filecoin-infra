import * as ed25519 from '@ucanto/principal/ed25519'
import * as DID from '@ipld/dag-ucan/did'
import { Aggregate } from '@web3-storage/aggregate-client'

/**
 * @param {import('../types').StorefrontSignerCtx} serviceSignerCtx
 * @param {ed25519.ConnectionView<any>} aggregationServiceConnection
 */
export async function createAggregateService (serviceSignerCtx, aggregationServiceConnection) {
  const issuer = getStorefrontSigner(serviceSignerCtx)
  const audience = aggregationServiceConnection.id

  /** @type {import('@web3-storage/aggregate-client/types').InvocationConfig} */
  const InvocationConfig = {
    issuer,
    audience,
    with: issuer.did(),
  }

  return {
    /**
     * 
     * @param {import('@web3-storage/aggregate-client/types').Offer[]} offers 
     */
    offer: async function (offers) {
      return await Aggregate.aggregateOffer(
        InvocationConfig,
        offers,
        { connection: aggregationServiceConnection }
      )
    }
  }
}

/**
 * @param {import('../types').StorefrontSignerCtx} config 
 */
function getStorefrontSigner(config) {
  const signer = ed25519.parse(config.PRIVATE_KEY)
  if (config.DID) {
    const did = DID.parse(config.DID).did()
    return signer.withDID(did)
  }
  return signer
}
