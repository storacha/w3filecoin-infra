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
