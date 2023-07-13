import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3'

import { ContentResolverError } from './errors.js'
import { parseContentSource, sortContentSources } from './utils.js'

/**
 * @typedef {object} S3Props
 * @property {import('@aws-sdk/client-s3').S3ClientConfig} clientOpts
 * 
 * @typedef {object} R2Props
 * @property {string} httpEndpoint
 */

/**
 * Content fetcher enables fetching content from S3 and R2 buckets based on the intentions of this pipeline.
 * This pipeline runs in AWS, and consequently will perform faster (and with free reads) when reading
 * directly from AWS account buckets via S3 client. However, content might not be in S3 as we also move out
 * of S3 writes. At that point, we should rely on R2 content sources, which we can also get free reads
 * via roundabout HTTP endpoint (instead of relying on S3 Client for R2).
 *
 * @param {S3Props} s3Props
 * @param {R2Props} r2Props
 * @returns {import('../types.js').ContentResolver}
 */
export function createContentResolver (s3Props, r2Props) {
  return {
    resolve: async (item) => {
      const sources = item.source
        .map(parseContentSource)
        // Prioritize reading from S3 if possible given we are running right there
        .sort(sortContentSources)

      // Return first successful response
      for (const source of sources) {
        if (source.provider === 's3') {
          const s3Response = await getFromS3(source, s3Props)
          if (s3Response) {
            return {
              ok: s3Response
            }
          }
        } else if (source.provider === 'r2') {
          const r2Response = await getFromR2(source, r2Props)
          if (r2Response) {
            return {
              ok: r2Response
            }
          }
        }
      }

      return {
        error: new ContentResolverError()
      }
    }
  }
}

/**
 * Attempt to get from one of the S3 content sources.
 *
 * @param {import('../types').ContentSource} contentSource
 * @param {S3Props} s3Props
 */
async function getFromS3(contentSource, s3Props) {
  const client = new S3Client({
    region: contentSource.bucketRegion,
    ...s3Props.clientOpts
  })
  const cmd = new GetObjectCommand({
    Key: contentSource.key,
    Bucket: contentSource.bucketName,
  })

  try {
    const response = await client.send(cmd)
    return await response.Body?.transformToByteArray()
  } catch {}
}

/**
 * @param {import('../types').ContentSource} contentSource
 * @param {R2Props} r2Props
 */
async function getFromR2(contentSource, r2Props) {
  const url = new URL(`key/${contentSource.key}`, r2Props.httpEndpoint)

  try {
    const res = await fetch(url)
    if (res.ok) {
      const data = await res.arrayBuffer()
      return new Uint8Array(data)
    }
  } catch {}
}
