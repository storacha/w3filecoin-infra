import { Piece } from '@web3-storage/data-segment'
import * as DID from '@ipld/dag-ucan/did'
import * as Signer from '@ucanto/principal/ed25519'
import { DealTracker } from '@web3-storage/filecoin-client'

const pieceString = process.argv[2]
if (!pieceString) {
  throw new Error('no aggregate piece was provided')
}

const storefront = await Signer.generate()
const res = await DealTracker.dealInfo(
  {
    issuer: storefront,
    with: storefront.did(),
    audience: DID.parse('did:web:web3.storage'),
  },
  Piece.fromString(pieceString).link
)

console.log(`known information for aggregate ${pieceString}:`)
console.log(res.out.ok)
