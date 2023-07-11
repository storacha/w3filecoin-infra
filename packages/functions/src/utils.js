import { parse as parseLink } from 'multiformats/link'

/**
 * @param {string} name 
 * @returns {string}
 */
export function mustGetEnv (name) {
  const value = process.env[name]
  if (!value) throw new Error(`Missing env var: ${name}`)
  return value
}

/**
 * @typedef {import('@w3filecoin/core/src/types').Content} Content
 * @typedef {import('@w3filecoin/core/src/types').Inserted<Content>} InsertedContent
 */

/**
 * Extract an EventRecord from the passed SQS Event
 *
 * @param {import('aws-lambda').SQSEvent} sqsEvent
 * @returns {InsertedContent | undefined}
 */
export function parseContentQueueEvent(sqsEvent) {
  if (sqsEvent.Records.length !== 1) {
    throw new Error(
      `Expected 1 event per invocation but received ${sqsEvent.Records.length}`
    )
  }

  const body = sqsEvent.Records[0].body
  if (!body) {
    return
  }
  const { link, size, source, inserted } = JSON.parse(body)

  return {
    link: parseLink(link),
    size,
    source,
    inserted,
  }
}
