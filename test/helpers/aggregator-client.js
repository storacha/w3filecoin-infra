import * as Signer from '@ucanto/principal/ed25519'
import * as DID from '@ipld/dag-ucan/did'
import { CAR, HTTP } from '@ucanto/transport'
import { connect } from '@ucanto/client'

/**
 * 
 * @param {URL} url 
 */
export async function getAggregatorClientConfig (url) {
  // UCAN actors
  const storefront = await Signer.generate()
  const aggregatorService = DID.parse('did:key:z6Mkq5KT7MGL8LCWiv6DNDpxKoXxg7NjFZFBFnkgRvXMaoxV')

  return {
    invocationConfig: {
      issuer: storefront,
      with: storefront.did(),
      audience: aggregatorService,
    },
    connection: connect({
      id: aggregatorService,
      codec: CAR.outbound,
      channel: HTTP.open({
        url,
        method: 'POST',
      }),
    })
  }
}
