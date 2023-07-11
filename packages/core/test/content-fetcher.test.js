import { testContentFetcher as test } from './helpers/context.js'
import { PutObjectCommand } from '@aws-sdk/client-s3'
import http from 'http'

import { createContentFetcher } from '../src/content-fetcher/index.js'
import { ContentFetcherErrorName } from '../src/content-fetcher/errors.js'

import { createBucket } from './helpers/resources.js'
import { getCargo } from './helpers/cargo.js'

test.beforeEach(async t => {
  const s3 = await createBucket()

  Object.assign(t.context, {
    s3Client: s3.client,
    bucketName: s3.bucketName,
    s3ClientOpts: s3.clientOpts
  })
})

test('should get from S3 first', async t => {
  const { s3Client, bucketName, s3ClientOpts } = t.context
  const [cargo] = await getCargo(1)
  const url = 'https://example.com'

  // Write to Bucket
  const putCmd = new PutObjectCommand({
    Key: cargo.content.link.toString(),
    Body: cargo.content.bytes,
    Bucket: bucketName
  })
  await s3Client.send(putCmd)

  /** @type {import('../src/types').ContentSource[]} */
  const source = [
    {
      bucketName,
      bucketRegion: 'us-west-2',
      key: cargo.content.link.toString()
    },
    {
      bucketName,
      bucketRegion: 'auto',
      key: `${cargo.content.link.toString()}/${cargo.content.link.toString()}`,
    }
  ]
  const item = {
    source,
    link: cargo.content.link,
    size: cargo.content.size
  }

  const contentFetcher = createContentFetcher(
    { clientOpts: s3ClientOpts },
    { httpEndpoint: url },
    { buckets: [bucketName]}
  )
  const { ok, error } = await contentFetcher.fetch(item)

  t.truthy(ok)
  t.falsy(error)
})

test('should get from R2 if not available via S3', async t => {
  const { bucketName, s3ClientOpts } = t.context
  const [cargo] = await getCargo(1)

  // HTTP Server to fetch content from R2
  const port = 8888
  const server = http.createServer(
    (_, response) => {
      response.writeHead(200)
      response.write(cargo.content.bytes)
      response.end()
    }
  )
  await new Promise(resolve => server.listen(port, () => resolve(true)))

  /** @type {import('../src/types').ContentSource[]} */
  const source = [
    {
      bucketName,
      bucketRegion: 'us-west-2',
      key: cargo.content.link.toString()
    },
    {
      bucketName,
      bucketRegion: 'auto',
      key: `${cargo.content.link.toString()}/${cargo.content.link.toString()}`,
    }
  ]
  const item = {
    source,
    link: cargo.content.link,
    size: cargo.content.size
  }

  const contentFetcher = createContentFetcher(
    { clientOpts: s3ClientOpts },
    { httpEndpoint: `http://localhost:${port}` },
    { buckets: [bucketName]}
  )
  const { ok, error } = await contentFetcher.fetch(item)
  t.truthy(ok)
  t.falsy(error)

  await new Promise(resolve => server.close(resolve))
})

test('should fail if there are no available sources', async t => {
  const { bucketName, s3ClientOpts } = t.context
  const [cargo] = await getCargo(1)
  const url = 'https://example.com'

  /** @type {import('../src/types').ContentSource[]} */
  const source = []
  const item = {
    source,
    link: cargo.content.link,
    size: cargo.content.size
  }

  const contentFetcher = createContentFetcher(
    { clientOpts: s3ClientOpts },
    { httpEndpoint: url },
    { buckets: [bucketName]}
  )
  const { ok, error } = await contentFetcher.fetch(item)

  t.falsy(ok)
  t.truthy(error)
  t.is(error?.name, ContentFetcherErrorName)
})

test('should fail if not possible to get from any of the available sources', async t => {
  const { bucketName, s3ClientOpts } = t.context
  const [cargo] = await getCargo(1)
  const url = 'http://localhost:1234'

  /** @type {import('../src/types').ContentSource[]} */
  const source = [
    {
      bucketName,
      bucketRegion: 'us-west-2',
      key: cargo.content.link.toString()
    },
    {
      bucketName,
      bucketRegion: 'auto',
      key: `${cargo.content.link.toString()}/${cargo.content.link.toString()}`,
    }
  ]
  const item = {
    source,
    link: cargo.content.link,
    size: cargo.content.size
  }

  const contentFetcher = createContentFetcher(
    { clientOpts: s3ClientOpts },
    { httpEndpoint: url },
    { buckets: [bucketName]}
  )
  const { ok, error } = await contentFetcher.fetch(item)

  t.falsy(ok)
  t.truthy(error)
  t.is(error?.name, ContentFetcherErrorName)
})
