import { webcrypto } from 'crypto'
import { CarWriter } from '@ipld/car'
import { CID } from 'multiformats/cid'
import * as raw from 'multiformats/codecs/raw'
import { sha256 } from 'multiformats/hashes/sha2'
import * as CAR from '@ucanto/transport/car'
import { CommP } from '@web3-storage/data-segment'

/**
 * @param {number} length
 */
export async function getCargo (length) {
  const cars = await Promise.all(Array.from({ length }).map(() => randomCAR(128)))

  return Promise.all(cars.map(async car => {
    const commP = CommP.build(car.bytes)

    return {
      piece: {
        link: commP.link(),
        size: commP.pieceSize,
      },
      content: {
        link: car.cid.link(),
        size: car.size,
        source: [
          {
            bucketName: 'carpark-prod-0',
            bucketRegion: 'auto',
            key: `${car.cid.link()}/${car.cid.link()}.car`,
            bucketEndpoint: 'https://pub-92584e4edae340ac9a75ebb3a34b47c2.r2.dev',
          },
          {
            bucketName: 'carpark-prod-0',
            bucketRegion: 'us-est-2',
            key: `${car.cid.link()}/${car.cid.link()}.car`,
          }
        ],
        bytes: car.bytes
      }
    }
  }))
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
