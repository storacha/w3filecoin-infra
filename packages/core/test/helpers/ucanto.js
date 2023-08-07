import * as Signer from '@ucanto/principal/ed25519'
import * as Server from '@ucanto/server'
import * as Client from '@ucanto/client'
import * as CAR from '@ucanto/transport/car'
import * as FilecoinCapabilities from '@web3-storage/capabilities/filecoin'

import { OperationFailed } from './errors.js'
import { mockService } from './mocks.js'

const nop = (/** @type {any} */ invCap) => {}

/**
 * @param {any} serviceProvider
 * @param {object} [options]
 * @param {(inCap: any) => void} [options.onCall]
 * @param {boolean} [options.mustFail]
 */
export async function getBrokerServiceServer (serviceProvider, options = {}) {
  const onCall = options.onCall || nop

  /** @type {import('@web3-storage/capabilities/types').AggregateAddSuccess} */
  const aggregateAddResponse = {
    status: 'queued',
  }

  const service = mockService({
    aggregate: {
      add: Server.provideAdvanced({
        capability: FilecoinCapabilities.aggregateAdd,
        handler: async ({ invocation, context }) => {
          const invCap = invocation.capabilities[0]

          if (!invCap.nb) {
            throw new Error('no nb field received in invocation')
          }

          if (options.mustFail) {
            return {
              error: new OperationFailed(
                'failed to add to aggregate',
                invCap.nb.piece
              )
            }
          }

          // Create effect for receipt with self signed queued operation
          const fx = await FilecoinCapabilities.aggregateAdd
          .invoke({
            issuer: context.id,
            audience: context.id,
            with: context.id.did(),
            nb: invCap.nb,
          })
          .delegate()

          onCall(invCap)

          return Server.ok(aggregateAddResponse).join(fx.link())
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

export async function getBrokerServiceCtx () {
  const aggregator = await Signer.generate()
  const broker = await Signer.generate()
  
  return {
    aggregator: {
      did: aggregator.did(),
      privateKey: Signer.format(aggregator),
      raw: aggregator
    },
    broker: {
      did: broker.did(),
      privateKey: Signer.format(broker),
      raw: broker
    }
  }
}
