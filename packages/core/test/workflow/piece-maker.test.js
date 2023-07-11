import { testWorkflow as test } from '../helpers/context.js'

import { Consumer } from 'sqs-consumer'
import pWaitFor from 'p-wait-for'
import { parse as parseLink } from 'multiformats/link'

import { createContentQueue } from '../../src/queue/content.js'
import { createPieceQueue } from '../../src/queue/piece.js'
import { ContentFetcherError } from '../../src/content-fetcher/errors.js'
import * as pieceMakerWorkflow from '../../src/workflow/piece-maker.js'

import { createDatabase, createBucket, createQueue } from '../helpers/resources.js'
import { getCargo } from '../helpers/cargo.js'

test.beforeEach(async t => {
  // Await for closing sometimes not fast enough
  await new Promise(resolve => setTimeout(() => resolve(true), 300))
  const sqs = await createQueue()
  const s3 = await createBucket()

  /** @type {import('@aws-sdk/client-sqs').Message[]} */
  const queueMessages = []
  const queueConsumer = Consumer.create({
    queueUrl: sqs.queueUrl,
    sqs: sqs.client,
    handleMessage: (message) => {
      queueMessages.push(message)
      return Promise.resolve()
    }
  })

  Object.assign(t.context, {
    dbClient: (await createDatabase()).client,
    s3Client: s3.client,
    sqsClient: sqs.client,
    bucketName: s3.bucketName,
    queueName: sqs.queueName,
    queueUrl: sqs.queueUrl,
    queueConsumer,
    queueMessages
  })
})

test.beforeEach(async t => {
  t.context.queueConsumer.start()
  await pWaitFor(() => t.context.queueConsumer.isRunning)
})

test.afterEach(t => {
  t.context.queueConsumer.stop()
})

test('can consume empty content queue', async t => {
  const { dbClient, sqsClient, queueUrl, queueMessages } = t.context
  const contentQueue = createContentQueue(dbClient)

  const { error, ok } = await pieceMakerWorkflow.consumer({
    contentQueue,
    sqsClient,
    queueUrl
  })
  t.falsy(error)
  t.is(ok?.count, 0)
  t.is(queueMessages.length, 0)
})

test('can consume content queue with valid content', async t => {
  const { dbClient, sqsClient, queueUrl, queueMessages } = t.context
  const contentQueue = createContentQueue(dbClient)
  const cargoItems = await getCargo(10)

  // Put content
  const contentQueuePutResp = await Promise.all(
    cargoItems.map(item => contentQueue.put(item.content))
  )
  t.falsy(contentQueuePutResp.find(resp => resp.error))

  // Get queued items
  const queuedItems = await contentQueue.peek()
  t.falsy(queuedItems.error)

  // Process queued items
  const { error, ok } = await pieceMakerWorkflow.consumer({
    contentQueue,
    sqsClient,
    queueUrl
  })
  t.falsy(error)
  await pWaitFor(() =>
    ok?.count === queuedItems.ok?.length &&
    ok?.count === queueMessages.length,
    { interval: 200 }
  )

  // Validate messages
  const contentItemsToProcess = queueMessages.map(qm => {
    const decoded = JSON.parse(qm.Body || '')
    return /** @type {import('../../src/types.js').Content} */ ({
      ...decoded,
      link: parseLink(decoded.link)
    })
  })

  for (const item of contentItemsToProcess) {
    t.truthy(queuedItems.ok?.find(qi => qi.link.equals(item.link)))
  }
})

test('can consume content queue and write to producer queue', async t => {
  const { dbClient, sqsClient, queueUrl, queueMessages } = t.context
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const cargoItems = await getCargo(10)

  // Put content
  const contentQueuePutResp = await Promise.all(
    cargoItems.map(item => contentQueue.put(item.content))
  )
  t.falsy(contentQueuePutResp.find(resp => resp.error))

  // Process queued items
  const { error, ok } = await pieceMakerWorkflow.consumer({
    contentQueue,
    sqsClient,
    queueUrl
  })
  t.falsy(error)
  await pWaitFor(() =>
    ok?.count === cargoItems.length &&
    ok?.count === queueMessages.length,
    { interval: 200 }
  )

  const contentItemsToProcess = normalizeContentItemsFromQueueMessages(queueMessages)
  // create content fetcher from memory fixtures
  const contentFetcher = createContentFetcher(cargoItems)

  // Process each content item
  for (const item of contentItemsToProcess) {
    const { error } = await pieceMakerWorkflow.producer({
      item,
      pieceQueue,
      contentFetcher
    })
    t.falsy(error)
  }

  // Cargo queue has entries to be aggregated
  const queuedCargoToAggregationAfterPieceCompute = await pieceQueue.peek()
  if (!queuedCargoToAggregationAfterPieceCompute.ok) {
    throw new Error('should have queued cargo to aggregate')
  }
  t.is(queuedCargoToAggregationAfterPieceCompute.ok?.length, cargoItems.length)

  // Validate pieces in destination queue were well derived
  for (const cargo of queuedCargoToAggregationAfterPieceCompute.ok) {
    t.truthy(cargoItems.find(ci => ci.piece.link.equals(cargo.piece)))
  }
})

test('can produce items gracefully when concurrently handling messages', async t => {
  const { dbClient, sqsClient, queueUrl, queueMessages } = t.context
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const cargoItems = await getCargo(10)

  // Put content
  const contentQueuePutResp = await Promise.all(
    cargoItems.map(item => contentQueue.put(item.content))
  )
  t.falsy(contentQueuePutResp.find(resp => resp.error))

  // Process queued items
  const { error, ok } = await pieceMakerWorkflow.consumer({
    contentQueue,
    sqsClient,
    queueUrl
  })
  t.falsy(error)
  await pWaitFor(() =>
    ok?.count === cargoItems.length &&
    ok?.count === queueMessages.length,
    { interval: 200 }
  )
  const contentItemsToProcess = normalizeContentItemsFromQueueMessages(queueMessages)

  // create content fetcher from memory fixtures
  const contentFetcher = createContentFetcher(cargoItems)

  // Process each content item
  const res = await Promise.all([
    pieceMakerWorkflow.producer({
      item: contentItemsToProcess[0],
      pieceQueue,
      contentFetcher
    }),
    pieceMakerWorkflow.producer({
      item: contentItemsToProcess[0],
      pieceQueue,
      contentFetcher
    })
  ])
  t.falsy(res.find(r => r.error))
})

test('producer fails to put in piece queue when content not set', async t => {
  const { dbClient } = t.context
  const pieceQueue = createPieceQueue(dbClient)
  const cargoItems = await getCargo(10)

  // create content fetcher from memory fixtures
  const contentFetcher = createContentFetcher(cargoItems)

  const { error } = await pieceMakerWorkflow.producer({
    item: cargoItems[0].content,
    pieceQueue,
    contentFetcher
  })

  t.truthy(error)
})

test('producer fails to put in piece queue when content fetcher cannot fetch content', async t => {
  const { dbClient, sqsClient, queueUrl, queueMessages } = t.context
  const contentQueue = createContentQueue(dbClient)
  const pieceQueue = createPieceQueue(dbClient)
  const cargoItems = await getCargo(10)

  // Put content
  const contentQueuePutResp = await Promise.all(
    cargoItems.map(item => contentQueue.put(item.content))
  )
  t.falsy(contentQueuePutResp.find(resp => resp.error))

  // Process queued items
  const { error, ok } = await pieceMakerWorkflow.consumer({
    contentQueue,
    sqsClient,
    queueUrl
  })
  t.falsy(error)
  await pWaitFor(() =>
    ok?.count === cargoItems.length &&
    ok?.count === queueMessages.length,
    { interval: 200 }
  )

  const contentItemsToProcess = normalizeContentItemsFromQueueMessages(queueMessages)
  // create content fetcher without fixtures
  const contentFetcher = createContentFetcher([])

  // Process each content item
  const { error: producerError } = await pieceMakerWorkflow.producer({
    item: contentItemsToProcess[0],
    pieceQueue,
    contentFetcher
  })
  t.truthy(producerError)
})

/**
 * 
 * @param {import('@aws-sdk/client-sqs').Message[]} queueMessages 
 * @returns 
 */
function normalizeContentItemsFromQueueMessages (queueMessages) {
  return queueMessages.map(qm => {
    const decoded = JSON.parse(qm.Body || '')
    return /** @type {import('../../src/types.js').Content} */ ({
      ...decoded,
      link: parseLink(decoded.link)
    })
  })
}

/**
 * @param {any[]} cargoItems
 */
function createContentFetcher (cargoItems) {
  /** @type {import('../../src/types.js').ContentFetcher} */
  const contentFetcher = {
    fetch: async function (item) {
      const cargo = cargoItems.find(cargo => cargo.content.link.equals(item.link))

      if (!cargo) {
        return {
          error: new ContentFetcherError()
        }
      }
      return {
        ok: cargo.content.bytes
      }
    }
  }

  return contentFetcher
}
