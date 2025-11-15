import * as Sentry from '@sentry/serverless'
import crypto from 'node:crypto'
import { createClient as createBufferStoreClient } from '@w3filecoin/core/src/store/aggregator-buffer-store.js'
import {
  createClient as createBufferQueueClient,
  decodeMessage
} from '@w3filecoin/core/src/queue/buffer-queue.js'
import { createClient as createAggregateOfferQueueClient } from '@w3filecoin/core/src/queue/aggregate-offer-queue.js'
import * as aggregatorEvents from '@storacha/filecoin-api/aggregator/events'
import { Piece } from '@web3-storage/data-segment'
import { LRUCache } from 'lru-cache'
import * as Digest from 'multiformats/hashes/digest'
import { sha256 } from 'multiformats/hashes/sha2'
import { mustGetEnv } from '../utils.js'

Sentry.AWSLambda.init({
  environment: process.env.SST_STAGE,
  dsn: process.env.SENTRY_DSN,
  tracesSampleRate: 0
})

/**
 * On buffer queue messages, reduce received buffer records into a bigger buffer.
 * - If new buffer does not have enough load to build an aggregate, it is stored
 * and requeued for buffer reducing
 * - If new buffer has enough load to build an aggregate, it is stored and queued
 * into aggregateOfferQueue. Remaining of the new buffer (in case buffer bigger
 * than maximum aggregate size) is re-queued into the buffer queue.
 *
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 */
async function handleBufferQueueMessage (sqsEvent) {
  // if one we should put back in queue
  if (sqsEvent.Records.length === 1) {
    return {
      batchItemFailures: sqsEvent.Records.map((r) => ({
        itemIdentifier: r.messageId
      }))
    }
  }

  // unexpected number of records
  if (sqsEvent.Records.length === 0) {
    return {
      statusCode: 400,
      body: `Expected 2 SQS messages per invocation but received ${sqsEvent.Records.length}`
    }
  }

  // Get context
  const context = getContext()
  // Parse records
  const recordsWithId = sqsEvent.Records.map((r) => {
    return {
      messageId: r.messageId,
      message: decodeMessage({
        MessageBody: r.body
      })
    }
  })

  const recordsByGroup = recordsWithId.reduce((acc, rec) => {
    const group = rec.message.group || 'default'
    if (!acc[group]) {
      acc[group] = []
    }
    acc[group].push(rec)
    return acc
  }, /** @type {Record<string, typeof recordsWithId[0][]>} */ ({}))

  const { filteredRecords, batchItemFailures } = Object.values(
    recordsByGroup
  ).reduce(
    (acc, records) => {
      if (records.length > 1) {
        acc.filteredRecords.push(...records.map((r) => r.message))
      } else {
        for (const r of records) {
          acc.batchItemFailures.push({
            itemIdentifier: r.messageId
          })
        }
      }
      return acc
    },
    {
      filteredRecords:
      /** @type {import('@storacha/filecoin-api/aggregator/api').BufferMessage[]} */ ([]),
      batchItemFailures: /** @type {{itemIdentifier: string}[]} */ ([])
    }
  )
  if (filteredRecords.length === 0) {
    return {
      batchItemFailures
    }
  }
  const { ok, error } = await aggregatorEvents.handleBufferQueueMessage(
    context,
    filteredRecords
  )
  if (error) {
    console.log('error', error)
    return {
      statusCode: 500,
      body: error.message || 'failed to handle buffer queue message'
    }
  }
  console.log('ok', ok)

  return {
    batchItemFailures,
    statusCode: 200,
    body: ok
  }
}

/** @type {LRUCache<string, import('@storacha/filecoin-api/aggregator/api').BufferRecord>} */
const bufferStoreCache = new LRUCache({ max: 10_000 })

/**
 * @param {import('@storacha/filecoin-api/aggregator/api').BufferStore} bufferStore
 * @param {LRUCache<string, import('@storacha/filecoin-api/aggregator/api').BufferRecord>} cache
 * @returns {import('@storacha/filecoin-api/aggregator/api').BufferStore}
 */
const withBufferStoreCache = (bufferStore, cache) => {
  return {
    ...bufferStore,
    async put (rec) {
      const res = await bufferStore.put(rec)
      if (res.ok) cache.set(rec.block.toString(), rec)
      return res
    },
    async get (key) {
      const cacheKey = key.toString()
      const cached = cache.get(cacheKey)
      if (cached) return { ok: cached }
      const res = await bufferStore.get(key)
      if (res.ok) cache.set(cacheKey, res.ok)
      return res
    }
  }
}

function getContext () {
  const {
    bufferStoreBucketName,
    bufferStoreBucketRegion,
    bufferQueueUrl,
    bufferQueueRegion,
    aggregateOfferQueueUrl,
    aggregateOfferQueueRegion,
    maxAggregateSize,
    minAggregateSize,
    minUtilizationFactor,
    maxAggregatePieces
  } = getEnv()

  return {
    bufferStore: withBufferStoreCache(
      createBufferStoreClient(
        { region: bufferStoreBucketRegion },
        { name: bufferStoreBucketName }
      ),
      bufferStoreCache
    ),
    bufferQueue: createBufferQueueClient(
      { region: bufferQueueRegion },
      { queueUrl: bufferQueueUrl }
    ),
    aggregateOfferQueue: createAggregateOfferQueueClient(
      { region: aggregateOfferQueueRegion },
      { queueUrl: aggregateOfferQueueUrl }
    ),
    config: {
      maxAggregateSize,
      maxAggregatePieces,
      minAggregateSize,
      minUtilizationFactor,
      prependBufferedPieces: [
        {
          // Small piece to prepend that is encoded as a CAR file
          piece: Piece.fromString(
            'bafkzcibciab3bwd67rgcoiejigar34jguwfasa5327hq3sjdcma3zz2ccupy4oi'
          ).link,
          // will be prepended, so policy is irrelevant
          policy:
          /** @type {import('@storacha/filecoin-api/aggregator/api').PiecePolicy} */ (
            0
          ),
          insertedAt: new Date().toISOString()
        }
      ],
      hasher: {
        name: sha256.name,
        code: sha256.code,
        /** @param {Uint8Array} bytes */
        digest: (bytes) => {
          // @ts-expect-error only available in node.js 20
          const digest = crypto.hash('sha256', bytes, 'buffer')
          return Digest.create(sha256.code, digest)
        }
      }
    }
  }
}

/**
 * Get Env validating it is set.
 */
function getEnv () {
  return {
    bufferStoreBucketName: mustGetEnv('BUFFER_STORE_BUCKET_NAME'),
    bufferStoreBucketRegion: mustGetEnv('AWS_REGION'),
    bufferQueueUrl: mustGetEnv('BUFFER_QUEUE_URL'),
    bufferQueueRegion: mustGetEnv('AWS_REGION'),
    aggregateOfferQueueUrl: mustGetEnv('AGGREGATE_OFFER_QUEUE_URL'),
    aggregateOfferQueueRegion: mustGetEnv('AWS_REGION'),
    maxAggregateSize: Number.parseInt(mustGetEnv('MAX_AGGREGATE_SIZE')),
    maxAggregatePieces: process.env.MAX_AGGREGATE_PIECES
      ? Number.parseInt(process.env.MAX_AGGREGATE_PIECES)
      : undefined,
    minAggregateSize: Number.parseInt(mustGetEnv('MIN_AGGREGATE_SIZE')),
    minUtilizationFactor: Number.parseInt(mustGetEnv('MIN_UTILIZATION_FACTOR'))
  }
}

export const main = Sentry.AWSLambda.wrapHandler(handleBufferQueueMessage)
