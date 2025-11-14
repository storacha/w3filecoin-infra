import { HeadObjectCommand } from '@aws-sdk/client-s3'

import { connectBucket } from '@w3filecoin/core/src/store/index.js'

const AWS_REGION = 'us-west-2'
const spadeOracleUrl = 'https://api.spade.storage/public/daghaus_active_replicas.json.zst'

const bucketClient = connectBucket({
  region: AWS_REGION
})
const putCmd = new HeadObjectCommand({
  Bucket: 'prod-w3filecoin-deal-tracker-deal-archive-store-1',
  Key: encodeURIComponent(spadeOracleUrl)
})

let res
try {
  res = await bucketClient.send(putCmd)
} catch (/** @type {any} */ error) {
  if (error?.$metadata.httpStatusCode !== 404) {
    throw error
  }
}

if (!res) {
  console.log('Oracle file not found')
} else {
  console.log('Oracle Last Modified:', res.LastModified)
  console.log('Oracle Content Length:', res.ContentLength)
}
