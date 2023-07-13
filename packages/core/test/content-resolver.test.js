import { testContentFetcher as test } from './helpers/context.js'
import { PutObjectCommand } from '@aws-sdk/client-s3'
import http from 'http'

import { createContentResolver } from '../src/content-resolver/index.js'
import { ContentResolverErrorName } from '../src/content-resolver/errors.js'
import { parseContentSource, sortContentSources } from '../src/content-resolver/utils.js'

import { createBucket } from './helpers/resources.js'
import { getCargo, getR2ContentSource, getS3ContentSource } from './helpers/cargo.js'

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

  const source = [
    getS3ContentSource('us-west-2', bucketName, cargo.content.link.toString()),
    getR2ContentSource(bucketName, `${cargo.content.link.toString()}/${cargo.content.link.toString()}.car`)
  ]
  const item = {
    source,
    link: cargo.content.link,
    size: cargo.content.size
  }

  const contentFetcher = createContentResolver(
    { clientOpts: s3ClientOpts },
    { httpEndpoint: url }
  )
  const { ok, error } = await contentFetcher.resolve(item)

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

  const source = [
    getS3ContentSource('us-west-2', bucketName, cargo.content.link.toString()),
    getR2ContentSource(bucketName, `${cargo.content.link.toString()}/${cargo.content.link.toString()}.car`)
  ]
  const item = {
    source,
    link: cargo.content.link,
    size: cargo.content.size
  }

  const contentFetcher = createContentResolver(
    { clientOpts: s3ClientOpts },
    { httpEndpoint: `http://localhost:${port}` }
  )
  const { ok, error } = await contentFetcher.resolve(item)
  t.truthy(ok)
  t.falsy(error)

  await new Promise(resolve => server.close(resolve))
})

test('should fail if there are no available sources', async t => {
  const { s3ClientOpts } = t.context
  const [cargo] = await getCargo(1)
  const url = 'https://example.com'

  /** @type {URL[]} */
  const source = []
  const item = {
    source,
    link: cargo.content.link,
    size: cargo.content.size
  }

  const contentFetcher = createContentResolver(
    { clientOpts: s3ClientOpts },
    { httpEndpoint: url },
  )
  const { ok, error } = await contentFetcher.resolve(item)

  t.falsy(ok)
  t.truthy(error)
  t.is(error?.name, ContentResolverErrorName)
})

test('should fail if not possible to get from any of the available sources', async t => {
  const { bucketName, s3ClientOpts } = t.context
  const [cargo] = await getCargo(1)
  const url = 'http://localhost:1234'

  const source = [
    getS3ContentSource('us-west-2', bucketName, cargo.content.link.toString()),
    getR2ContentSource(bucketName, `${cargo.content.link.toString()}/${cargo.content.link.toString()}.car`)
  ]
  const item = {
    source,
    link: cargo.content.link,
    size: cargo.content.size
  }

  const contentFetcher = createContentResolver(
    { clientOpts: s3ClientOpts },
    { httpEndpoint: url }
  )
  const { ok, error } = await contentFetcher.resolve(item)

  t.falsy(ok)
  t.truthy(error)
  t.is(error?.name, ContentResolverErrorName)
})

test('content source parser should parse s3 URL', (t) => {
  const s3Url = new URL('https://carpark-staging-0.s3.us-east-2.amazonaws.com/bagbaiera2222anvq5hsvpi3ftbx4tojrjvdptd4q7jnhiny4ssntzwz3ed6q/bagbaiera2222anvq5hsvpi3ftbx4tojrjvdptd4q7jnhiny4ssntzwz3ed6q.car')
  const source = parseContentSource(s3Url)

  t.is(source.provider, 's3')
  t.is(source.bucketRegion, 'us-east-2')
  t.is(source.bucketName, 'carpark-staging-0')
  t.is(source.key, 'bagbaiera2222anvq5hsvpi3ftbx4tojrjvdptd4q7jnhiny4ssntzwz3ed6q/bagbaiera2222anvq5hsvpi3ftbx4tojrjvdptd4q7jnhiny4ssntzwz3ed6q.car')
})

test('content source parser should parse r2 URL', (t) => {
  const r2Url = new URL('https://fffa4b4363a7e5250af8357087263b3a.r2.cloudflarestorage.com/carpark-staging-0/bagbaiera2222anvq5hsvpi3ftbx4tojrjvdptd4q7jnhiny4ssntzwz3ed6q/bagbaiera2222anvq5hsvpi3ftbx4tojrjvdptd4q7jnhiny4ssntzwz3ed6q.car')
  const source = parseContentSource(r2Url)

  t.is(source.provider, 'r2')
  t.is(source.bucketRegion, 'auto')
  t.is(source.bucketName, 'carpark-staging-0')
  t.is(source.key, 'bagbaiera2222anvq5hsvpi3ftbx4tojrjvdptd4q7jnhiny4ssntzwz3ed6q/bagbaiera2222anvq5hsvpi3ftbx4tojrjvdptd4q7jnhiny4ssntzwz3ed6q.car')
})

test('content source parser should fail if a source is not a r2 or s3 url', (t) => {
  const badUrl = new URL('https://bafy.w3s.link')
  t.throws(() => parseContentSource(badUrl))
})

test('should sort to s3 first', t => {
  const contentSources = [
    parseContentSource(
      new URL('https://fffa4b4363a7e5250af8357087263b3a.r2.cloudflarestorage.com/carpark-staging-0/bagbaiera2222anvq5hsvpi3ftbx4tojrjvdptd4q7jnhiny4ssntzwz3ed6q/bagbaiera2222anvq5hsvpi3ftbx4tojrjvdptd4q7jnhiny4ssntzwz3ed6q.car')
    ),
    parseContentSource(
      new URL('https://carpark-staging-0.s3.us-east-2.amazonaws.com/bagbaiera2222anvq5hsvpi3ftbx4tojrjvdptd4q7jnhiny4ssntzwz3ed6q/bagbaiera2222anvq5hsvpi3ftbx4tojrjvdptd4q7jnhiny4ssntzwz3ed6q.car')
    )
  ].sort(sortContentSources)

  t.is(contentSources[0].provider, 's3')
  t.is(contentSources[1].provider, 'r2')
})
