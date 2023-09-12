import { GetObjectCommand } from '@aws-sdk/client-s3'
import pRetry from 'p-retry'

/**
 * @param {import('@aws-sdk/client-s3').S3Client} client
 * @param {string} bucketName
 * @param {string} key
 */
export async function waitForBucketItem (client, bucketName, key) {
  const cmd = new GetObjectCommand({
    Bucket: bucketName,
    Key: key
  })

  const response = await pRetry(async () => {
    let r
    try {
      r = await client.send(cmd)
    } catch (error) {
      // @ts-expect-error aws error no typed
      if (error?.$metadata?.httpStatusCode === 404) {
        throw new Error('not found')
      }
    }

    return r
  }, {
    retries: 10,
    maxTimeout: 1000,
    minTimeout: 1000
  })
  
  return await response?.Body?.transformToByteArray()
}
