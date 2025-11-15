import {
  S3Client,
  GetObjectCommand,
  ListObjectsV2Command
} from '@aws-sdk/client-s3'
import {
  RecordNotFound,
  StorageOperationFailed
} from '@storacha/upload-api/errors'
import * as CAR from '@ucanto/transport/car'
import { parseLink, Receipt } from '@ucanto/core'

/**
 * @typedef {import('@aws-sdk/client-s3').S3ClientConfig} Address
 * @typedef {S3Client} Channel
 *
 * @typedef {{
 * channel: Channel
 * address: Address
 * }} Connection
 *
 * @typedef {object} Buckets
 * @property {{name:string}} index
 * @property {{name:string}} message
 *
 * @typedef {object} Options
 * @property {Connection} connection
 * @property {string} region
 * @property {Buckets} buckets
 *
 * @typedef {object} Store
 * @property {Channel} channel
 * @property {string} region
 * @property {Buckets} buckets
 */

/**
 * @param {Options} options
 * @returns {Store}
 */
export const open = ({ connection, region, buckets }) => ({
  channel:
    connection.channel ?? new S3Client({ ...connection.address, region }),
  region,
  buckets
})

/**
 * Gets a receipt corresponding to the given task.
 *
 * @param {Store} store
 * @param {import('@ucanto/interface').UnknownLink} task
 * @returns {Promise<import('@ucanto/interface').Result<import('@ucanto/interface').Receipt, import('@ucanto/interface').Failure>>}
 */
export const getReceipt = async (store, task) => {
  const result = await load(store, { invocation: task })

  if (result.error) {
    return result
  }

  //
  const invocation =
    /** @type {import('@ucanto/interface').UCANLink<[import('@ucanto/interface').Capability]>} */ (
      task
    )
  const { ok: entry, error } = await resolve(store, {
    receipt: invocation
  })
  if (error) {
    return { error }
  }

  const { ok: body, error: readError } = await read(store, entry.message)
  if (readError) {
    return { error: readError }
  }

  if (entry.root) {
    const archive = await CAR.codec.decode(body)
    const receipt = Receipt.view({
      root: entry.root,
      blocks: archive.blocks
    })
    if (receipt) {
      // @ts-ignore
      return { ok: receipt }
    }
  } else {
    const message = await CAR.request.decode({
      body,
      headers: {}
    })

    // Attempt to find a receipt corresponding to this task
    const receipt = message.receipts.get(`${task}`)
    if (receipt) {
      // @ts-ignore
      return { ok: receipt }
    }
  }

  return {
    error: new RecordNotFound(
      `agent message ${entry.message} does not contain receipt for ${task} task`
    )
  }
}

/**
 * We may want to lookup an agent message by task that either contains
 * corresponding invocation or a receipt . This type describes a query
 * as variant of `invocation` and `receipt` types signaling which of the
 * two messages to lookup.
 *
 * @typedef {import('@ucanto/interface').Variant<{
 * invocation: import('@ucanto/interface').UnknownLink
 * receipt: import('@ucanto/interface').UnknownLink
 * }>}  AgentMessageQuery
 */

/**
 * Resolves paths to an agent message for the given key.
 *
 * @typedef {{root?: import('@ucanto/interface').Link, message:import('@ucanto/interface').Link}} IndexEntry
 *
 * @param {Store} store
 * @param {AgentMessageQuery} query
 * @returns {Promise<import('@ucanto/interface').Result<IndexEntry, RecordNotFound|StorageOperationFailed>>}
 */
export const resolve = async (store, { invocation, receipt }) => {
  // If we are resolving an invocation we need to find an INcoming message
  // which get `.in` suffix. If we are looking for a receipt we need to find
  // an OUTgoing message which get `.out` suffix.
  const [prefix, suffix] = invocation
    ? [`${invocation}/`, '.in']
    : [`${receipt}/`, '.out']

  // Previously we used to treat task and invocation as a same and used
  // following indexing using following pseudo symlinks
  //
  // ${invocation.cid}/${message.cid}.in
  // ${invocation.cid}/${message.cid}.out
  //
  // After we started distinguishing between task and invocation, we have
  // adopted following indexing instead
  // ${task.cid}/${invocation.cid}@${message.cid}.in
  // ${task.cid}/${invocation.cid}@${message.cid}.out
  //
  // Here we could be looking up old receipts or new ones which is why we
  // simply list all entries under the link (which is either task or invocation)
  // and then filter out by the prefix.
  const entries = await list(store, { prefix, suffix })
  if (entries.error) {
    return entries
  }

  // Prefer an entry containing a root link if non found return the first entry
  const [first, ...rest] = entries.ok
  const head = toIndexEntry(first)
  if (head.root) {
    return { ok: head }
  } else {
    for (const path of rest) {
      const entry = toIndexEntry(path)
      if (entry.root) {
        return { ok: entry }
      }
    }
    return { ok: head }
  }
}

/**
 * Loads a view based on the query
 *
 *
 * @param {Store} store
 * @param {AgentMessageQuery} query
 */
const load = async (store, query) => {
  const { ok: index, error } = await resolve(store, query)

  if (error) {
    return { error }
  }

  const { message, root } = index
  const { ok: bytes, error: readError } = await read(store, message)
  if (readError) {
    return { error: readError }
  }

  return {
    ok: {
      archive: await CAR.codec.decode(bytes),
      root: root ?? undefined
    }
  }
}
/**
 * Takes a pseudo symlink and extracts the DAG `root` and DAG `archive`
 * identifiers.
 *
 * @param {string} path
 * @returns {IndexEntry}
 */
const toIndexEntry = (path) => {
  const start = path.indexOf('/')
  const offset = path.indexOf('@')
  return offset > 0
    ? {
        root: parseLink(path.slice(start + 1, offset)),
        message: parseLink(path.slice(offset + 1, path.indexOf('.')))
      }
    : {
        message: parseLink(path.slice(start + 1, path.indexOf('.')))
      }
}

/**
 *
 * @param {Store} connection
 * @param {object} key
 * @param {string} key.prefix
 * @param {string} key.suffix
 * @returns {Promise<import('@ucanto/interface').Result<[string, ...string[]], RecordNotFound|StorageOperationFailed>>}
 */
const list = async (connection, { prefix, suffix }) => {
  try {
    const command = new ListObjectsV2Command({
      Bucket: connection.buckets.index.name,
      Prefix: prefix
    })

    const { Contents } = await connection.channel.send(command)
    const entries =
      Contents?.map((c) => c.Key ?? '').filter((key) => key.endsWith(suffix)) ??
      []

    return entries.length > 0
      ? { ok: /** @type {[string, ...string[]]} */ (entries) }
      : {
          error: new RecordNotFound(
            `no pseudo symlink matching query ${prefix}*${suffix} was found`
          )
        }
  } catch (/** @type {any} */ error) {
    if (error?.$metadata?.httpStatusCode === 404) {
      return {
        error: new RecordNotFound(
          `no pseudo symlink matching query ${prefix}*${suffix} was found`
        )
      }
    }
    return {
      error: new StorageOperationFailed(error.message)
    }
  }
}

/**
 * @param {Store} connection
 * @param {import('@ucanto/interface').Link} message
 * @returns {Promise<import('@ucanto/interface').Result<Uint8Array, RecordNotFound|StorageOperationFailed>>}
 */
const read = async ({ buckets, channel }, message) => {
  const getCmd = new GetObjectCommand({
    Bucket: buckets.message.name,
    Key: toMessagePath({ message })
  })

  let res
  try {
    res = await channel.send(getCmd)
  } catch (/** @type {any} */ error) {
    if (error?.$metadata?.httpStatusCode === 404) {
      return {
        error: new RecordNotFound(
          `agent message archive ${message} not found in store`
        )
      }
    }
    return {
      error: new StorageOperationFailed(error.message)
    }
  }
  if (!res || !res.Body) {
    return {
      error: new RecordNotFound(
        `agent message archive ${message} not found in store`
      )
    }
  }

  const bytes = await res.Body.transformToByteArray()

  return {
    ok: bytes
  }
}

/**
 * @param {Store} store
 * @param {import('@ucanto/interface').Link} message
 */
export const toMessageURL = (store, message) =>
  new URL(
    `https://${store.buckets.message.name}.s3.${
      store.region
    }.amazonaws.com/${toMessagePath({ message })}`
  )

/**
 * @param {object} source
 * @param {import('@ucanto/interface').Link} source.message
 */

export const toMessagePath = ({ message }) => `${message}/${message}`

/**
 * @param {object} source
 * @param {import('@ucanto/interface').Link} source.message
 * @param {import('@ucanto/interface').Link} source.task
 * @param {import('@ucanto/interface').Link} source.invocation
 */
export const toInvocationPath = ({ message, task, invocation }) =>
  `${task}/${invocation}@${message}.in`

/**
 * @param {object} source
 * @param {import('@ucanto/interface').Link} source.message
 * @param {import('@ucanto/interface').Link} source.task
 * @param {import('@ucanto/interface').Link} source.receipt
 */
export const toReceiptPath = ({ message, task, receipt }) =>
  `${task}/${receipt}@${message}.out`
