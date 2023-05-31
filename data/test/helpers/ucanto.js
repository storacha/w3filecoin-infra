import * as Signer from '@ucanto/principal/ed25519'
import * as Server from '@ucanto/server'
import * as Client from '@ucanto/client'
import * as CAR from '@ucanto/transport/car'
import * as AggregateCapabilities from '@web3-storage/capabilities/aggregate'
import * as OfferCapabilities from '@web3-storage/capabilities/offer'

import { mockService } from './mocks.js'

const nop = (/** @type {any} */ invCap) => {}

/**
 * @param {any} serviceProvider
 * @param {object} [options]
 * @param {(inCap: any) => void} [options.onCall]
 */
export async function getAggregateServiceServer (serviceProvider, options = {}) {
  const onCall = options.onCall || nop

  const service = mockService({
    aggregate: {
      offer: Server.provideAdvanced({
        capability: AggregateCapabilities.offer,
        // @ts-expect-error not failure type expected because of assert throw
        handler: async ({ invocation, context }) => {
          /** @type {import('@web3-storage/capabilities/types').AggregateOfferSuccess} */
          const aggregateOfferResponse = {
            status: 'queued',
          }
          const invCap = invocation.capabilities[0]

          // Create effect for receipt
          const fx = await OfferCapabilities.arrange
          .invoke({
            issuer: context.id,
            audience: context.id,
            with: context.id.did(),
            nb: {
              // @ts-expect-error no type for commitmentProof
              commitmentProof: invCap.nb?.commitmentProof,
            },
          })
          .delegate()

          onCall(invCap)

          return Server.ok(aggregateOfferResponse)
            .join(fx.link())
        }
      })
    }
  })

  const server = Server.create({
    id: serviceProvider,
    service,
    codec: CAR.inbound,
  })
  const connection = Client.connect({
    id: serviceProvider,
    codec: CAR.outbound,
    channel: server,
  })

  return {
    service,
    connection
  }
}

export async function getAggregateServiceCtx () {
  const aggregationService = await Signer.generate()
  const storefront = await Signer.generate()
  
  return {
    aggregationService: {
      DID: aggregationService.did(),
      PRIVATE_KEY: Signer.format(aggregationService),
      raw: aggregationService
    },
    storefront: {
      DID: storefront.did(),
      PRIVATE_KEY: Signer.format(storefront),
      raw: storefront
    }
  }
}
