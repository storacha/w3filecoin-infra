import { tesWorkflow as test } from '../helpers/context.js'
import {
  createS3,
  createBucket,
  createDynamodDb,
  createTable
} from '../helpers/resources.js'
import { randomAggregate } from '../helpers/cargo.js'
import { getDealerServiceServer, getDealerServiceCtx } from '../helpers/ucanto.js'
import { OperationErrorName } from '../helpers/errors.js'

import { StoreOperationErrorName } from '@web3-storage/filecoin-api/errors'
import pDefer from 'p-defer'

import { getServiceSigner } from '../../src/service.js'
import { encode as bufferEncode, decode as bufferDecode, encodeBlock } from '../../src/data/buffer.js'
import { encode as aggregateEncode, decode as aggregateDecode } from '../../src/data/aggregate.js'
import { createTableStoreClient } from '../../src/store/table-client.js'
import { createBucketStoreClient } from '../../src/store/bucket-client.js'
import { aggregateStoreTableProps } from '../../src/store/index.js'

import { dealerQueue } from '../../src/workflow/dealer-queue.js'

/**
 * @typedef {import('../../src/data/types.js').PiecePolicy} PiecePolicy
 */

test.beforeEach(async (t) => {
  const dynamo = await createDynamodDb()
  Object.assign(t.context, {
    s3: (await createS3()).client,
    dynamoClient: dynamo.client,
  })
})

test('can add produced aggregate', async t => {
  const {
    aggregateRecord,
    bufferStoreClient,
    aggregateStoreClient,
    buffer
  } = await getContext(t.context)

  // Store buffer used for aggregate
  await bufferStoreClient.put(buffer)

  const dealerQueueCall = pDefer()
  const { invocationConfig, dealerService } = await getService({
    onCall: dealerQueueCall
  })
  const dealerQueueResp = await dealerQueue({
    bufferStoreClient,
    aggregateStoreClient,
    aggregateRecord: await aggregateEncode.message(aggregateRecord),
    invocationConfig,
    dealerServiceConnection: dealerService.connection
  })

  t.truthy(dealerQueueResp.ok)
  t.falsy(dealerQueueResp.error)
  t.is(dealerQueueResp.ok, 1)

  // Validate ucanto server call
  t.is(dealerService.service.deal.queue.callCount, 1)
  const invCap = await dealerQueueCall.promise
  t.is(invCap.can, 'deal/queue')

  // TODO: validate CID of piece invCap.nb.piece
  // TODO: validate deal content invCap.nb.deal
  // TODO: validate offer CBOR block cid invCap.nb.offer
})

test('fails adding aggregate if fails to read from store', async t => {
  const {
    aggregateRecord,
    bufferStoreClient,
    aggregateStoreClient
  } = await getContext(t.context)

  const dealerQueueCall = pDefer()
  const { invocationConfig, dealerService } = await getService({
    onCall: dealerQueueCall
  })
  const dealerQueueResp = await dealerQueue({
    bufferStoreClient,
    aggregateStoreClient,
    aggregateRecord: await aggregateEncode.message(aggregateRecord),
    invocationConfig,
    dealerServiceConnection: dealerService.connection
  })

  t.falsy(dealerQueueResp.ok)
  t.truthy(dealerQueueResp.error)
  t.is(dealerQueueResp.error?.name, StoreOperationErrorName)

  // Validate ucanto server call
  t.is(dealerService.service.deal.queue.callCount, 0)
})

test('fails adding aggregate if fails to add to dealer', async t => {
  const {
    aggregateRecord,
    bufferStoreClient,
    aggregateStoreClient,
    buffer
  } = await getContext(t.context)

  // Store buffer used for aggregate
  await bufferStoreClient.put(buffer)

  const dealerQueueCall = pDefer()
  const { invocationConfig, dealerService } = await getService({
    onCall: dealerQueueCall,
    mustFail: true
  })
  const dealerQueueResp = await dealerQueue({
    bufferStoreClient,
    aggregateStoreClient,
    aggregateRecord: await aggregateEncode.message(aggregateRecord),
    invocationConfig,
    dealerServiceConnection: dealerService.connection
  })

  t.falsy(dealerQueueResp.ok)
  t.truthy(dealerQueueResp.error)
  t.is(dealerQueueResp.error?.name, OperationErrorName)

  // Validate ucanto server call
  t.is(dealerService.service.deal.queue.callCount, 1)
})

/**
 * @param {import('../helpers/context.js').BucketContext & import('../helpers/context.js').DbContext & import('../helpers/context.js').QueueContext} context
 */
async function getContext (context) {
  const { s3, dynamoClient } = context

  const { aggregate, pieces } = await randomAggregate(10, 128)
  const buffer = buildBuffer(pieces)
  const bufferCid = await encodeBlock(buffer)
  const bucketName = await createBucket(s3)
  const tableName = await createTable(dynamoClient, aggregateStoreTableProps)
  const aggregateRecord = {
    piece: aggregate.link,
    buffer: bufferCid.cid,
    insertedAt: Date.now(),
    storefront: 'did:web:web3.storage',
    group: 'did:web:free.web3.storage',
    stat: /** @type {import('../../src/data/types.js').AggregateStatus} */ (0),
  }

  const bufferStoreClient = createBucketStoreClient(s3, {
    name: bucketName,
    encodeRecord: bufferEncode.storeRecord,
    decodeRecord: bufferDecode.storeRecord,
  })
  const aggregateStoreClient = createTableStoreClient(dynamoClient, {
    tableName,
    encodeRecord: aggregateEncode.storeRecord,
    decodeRecord: aggregateDecode.storeRecord,
    encodeKey: aggregateEncode.storeKey
  })

  return {
    aggregateRecord,
    bufferStoreClient,
    aggregateStoreClient,
    buffer
  }
}

/**
 * @param {object} options
 * @param {import('p-defer').DeferredPromise<any>} options.onCall
 * @param {boolean} [options.mustFail]
 */
async function getService (options) {
  const { dealer, aggregator } = await getDealerServiceCtx()
  const dealerService = await getDealerServiceServer(dealer.raw, {
    onCall: (invCap) => {
      options.onCall.resolve(invCap)
    },
    mustFail: options.mustFail
  })
  const issuer = getServiceSigner(aggregator)
  const audience = dealerService.connection.id
  /** @type {import('@web3-storage/filecoin-client/types').InvocationConfig} */
  const invocationConfig = {
    issuer,
    audience,
    with: issuer.did(),
  }

  return {
    invocationConfig,
    dealerService
  }
}

/**
 * @param {{ link: import('@web3-storage/data-segment').PieceLink }[]} pieces
 */
function buildBuffer (pieces) {
  return {
    pieces: pieces.map(p => ({
      piece: p.link,
      policy: /** @type {PiecePolicy} */ (0),
      insertedAt: Date.now()
    })),
    storefront: 'did:web:web3.storage',
    group: 'did:web:free.web3.storage',
  }
}
