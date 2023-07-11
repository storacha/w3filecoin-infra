import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3'
import { ContentFetcherError } from './errors.js'

const S3_REGIONS = ['us-west-2', 'us-east-2']
const R2_REGIONS = ['auto']

const validBuckets = [
  'carpark-prod-0',
  'carpark-staging-0',
]

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
 * @param {object} [options]
 * @param {string[]} [options.buckets]
 * @returns {import('../types').ContentFetcher}
 */
export function createContentFetcher (s3Props, r2Props, options = {}) {
  const buckets = options.buckets || validBuckets

  return {
    fetch: async (item) => {
      // Prioritize reading from S3 if possible given we are running right there
      const s3sources = item.source.filter(source =>
        S3_REGIONS.includes(source.bucketRegion) && buckets.includes(source.bucketName)
      )
      const s3Response = await getFromS3(s3sources, s3Props)
      if (s3Response) {
        return {
          ok: s3Response
        }
      }

      // Read from R2 via HTTP endpoint if available source
      const r2source = item.source.find(source =>
        R2_REGIONS.includes(source.bucketRegion) && buckets.includes(source.bucketName)
      )
      if (r2source) {
        const r2Response = await getFromR2(item.link, r2Props)
        if (r2Response) {
          return {
            ok: r2Response
          }
        }
      }

      return {
        error: new ContentFetcherError()
      }
    }
  }
}

/**
 * Attempt to get from one of the S3 content sources.
 *
 * @param {import('../types').ContentSource[]} contentSources
 * @param {S3Props} s3Props
 */
async function getFromS3(contentSources, s3Props) {
  for (const contentSource of contentSources) {
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
}

/**
 * @param {import('multiformats').UnknownLink} link
 * @param {R2Props} r2Props
 */
async function getFromR2(link, r2Props) {
  const url = new URL(link.toString(), r2Props.httpEndpoint)

  try {
    const res = await fetch(url)
    if (res.ok) {
      const data = await res.arrayBuffer()
      return new Uint8Array(data)
    }
  } catch {}
}
