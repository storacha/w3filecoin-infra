/**
 * Parses source uri string into object.
 *
 * @param {URL} source
 * @returns {import('../types').ContentSource}
 */
export function parseContentSource (source) {
  if (source.toString().includes('amazonaws.com')) {
    return parseS3Source(source)
  } else if (source.toString().includes('cloudflarestorage.com')) {
    return parseR2Source(source)
  }
  throw new Error(`given source is not from s3 or r2 provider: ${source.toString()}`)
}

/**
 * Sorts content sources to try reading from s3 provider first.
 *
 * @param {import('../types').ContentSource} sourceA 
 * @param {import('../types').ContentSource} sourceB 
 */
export function sortContentSources (sourceA, sourceB) {
  if (sourceA.provider === 's3' && sourceB.provider === 'r2') {
    return -1
  } else if (sourceA.provider === 'r2' && sourceB.provider === 's3') {
    return 1
  }

  return 0
}

/**
 * 
 * @param {URL} source
 * @returns {import('../types').ContentSource}
 */
function parseS3Source (source) {
  // hostname 'carpark-prod-0.s3.us-west-2.amazonaws.com'
  const hostnameParts = source.hostname.match(/[^.]+/g)
  if (hostnameParts?.length !== 5) {
    throw new Error(`given s3 source is not valid: ${source.toString()}`)
  }
  return {
    provider: 's3',
    bucketName: hostnameParts[0],
    bucketRegion: hostnameParts[2],
    // pathname '/bagbaiera2222anvq5hsvpi3ftbx4tojrjvdptd4q7jnhiny4ssntzwz3ed7q/bagbaiera2222anvq5hsvpi3ftbx4tojrjvdptd4q7jnhiny4ssntzwz3ed7q.car'
    key: source.pathname.replace(/^\/(.*)/, '$1')
  }
}

/**
 * 
 * @param {URL} source
 * @returns {import('../types').ContentSource} 
 */
function parseR2Source (source) {
  // pathname '/carpark-prod-0/bagbaiera22222imnsq6z5hzgqh5hvj2qirxrofknvfpftcn2qcsqx6yx73tq/bagbaiera22222imnsq6z5hzgqh5hvj2qirxrofknvfpftcn2qcsqx6yx73tq.car'
  const [, bucketName, key] = source.pathname.split(/\/(.*?)\//)
  return {
    provider: 'r2',
    bucketName,
    bucketRegion: 'auto',
    key
  }
}
