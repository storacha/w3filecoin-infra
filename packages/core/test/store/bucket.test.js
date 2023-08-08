import { testStore as test } from '../helpers/context.js'
import {
  createS3,
  createBucket,
} from '../helpers/resources.js'
import { randomCargo } from '../helpers/cargo.js'

import { encode, decode } from '../../src/data/buffer.js'
import { createBucketStoreClient } from '../../src/store/bucket-client.js'

test.before(async (t) => {
  Object.assign(t.context, {
    s3: (await createS3()).client,
  })
})

test('can put and get buffer record', async t => {
  const { s3 } = t.context
  const bucketName = await createBucket(s3)
  const pieces = await randomCargo(1, 128)
  const storefront = 'did:web:web3.storage'
  const group = 'did:web:free.web3.storage'

  const bufferStore = createBucketStoreClient(s3, {
    name: bucketName,
    encodeRecord: encode.storeRecord,
    decodeRecord: decode.storeRecord,
  })
  t.truthy(bufferStore)

  const bufferedPieces = pieces.map(p => ({
    piece: p.link,
    insertedAt: Date.now(),
    policy: /** @type {import('../../src/data/types.js').PiecePolicy} */ (0),
  }))
  const buffer = {
    pieces: bufferedPieces,
    storefront,
    group,
  }
  const putRes = await bufferStore.put(buffer)
  t.truthy(putRes.ok)
  if (putRes.error) {
    throw new Error('failed to put to buffer store')
  }
  const key = await encode.storeKey(buffer)

  const getRes = await bufferStore.get(key)
  t.truthy(getRes.ok)
  t.falsy(getRes.error)
  t.is(buffer.storefront, storefront)
  t.is(buffer.group, group)
  t.is(buffer.pieces.length, bufferedPieces.length)
})
