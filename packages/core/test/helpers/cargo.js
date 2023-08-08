import { webcrypto } from 'crypto'
import { CarWriter } from '@ipld/car'
import { CID } from 'multiformats/cid'
import * as raw from 'multiformats/codecs/raw'
import { sha256 } from 'multiformats/hashes/sha2'
import * as CAR from '@ucanto/transport/car'
import { Aggregate, Piece } from '@web3-storage/data-segment'

/**
 * @param {number} length
 * @param {number} size
 */
export async function randomCargo(length, size) {
  const cars = await Promise.all(
    Array.from({ length }).map(() => randomCAR(size))
  )

  return cars.map((car) => {
    const piece = Piece.fromPayload(car.bytes)

    return {
      link: piece.link,
      height: piece.height,
      root: piece.root,
      content: car.cid
    }
  })
}

/**
 * @param {number} length
 * @param {number} size
 */
export async function randomAggregate(length, size) {
  const pieces = await randomCargo(length, size)

  const aggregateBuild = Aggregate.build({
    pieces,
  })

  

  return {
    pieces: pieces.map((p) => ({
      link: p.link,
      height: p.height,
      content: p.content,
    })),
    aggregate: aggregateBuild.toInfo(),
  }
}

/** @param {number} size */
async function randomBytes(size) {
  const bytes = new Uint8Array(size)
  while (size) {
    const chunk = new Uint8Array(Math.min(size, 65_536))
    webcrypto.getRandomValues(chunk)

    size -= bytes.length
    bytes.set(chunk, size)
  }
  return bytes
}

/** @param {number} size */
async function randomCAR(size) {
  const bytes = await randomBytes(size)
  const hash = await sha256.digest(bytes)
  const root = CID.create(1, raw.code, hash)

  const { writer, out } = CarWriter.create(root)
  writer.put({ cid: root, bytes })
  writer.close()

  const chunks = []
  for await (const chunk of out) {
    chunks.push(chunk)
  }
  const blob = new Blob(chunks)
  const cid = await CAR.codec.link(new Uint8Array(await blob.arrayBuffer()))

  return Object.assign(blob, { cid, roots: [root], bytes })
}

/**
 * @param {string} bucketRegion 
 * @param {string} bucketName 
 * @param {string} key 
 */
export function getS3ContentSource (bucketRegion, bucketName, key) {
  return new URL(`https://${bucketName}.s3.${bucketRegion}.amazonaws.com/${key}`)
}

/**
 * @param {string} bucketName
 * @param {string} key 
 */
export function getR2ContentSource (bucketName, key) {
  return new URL(`https://fffa4b4363a7e5250af8357087263b3a.r2.cloudflarestorage.com/${bucketName}/${key}`)
}
