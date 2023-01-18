/**
 * @param {import('aws-lambda').DynamoDBStreamEvent} event
 */
export function parseDynamoDbEvent (event) {
  return event.Records.map(r => ({
    new: r.dynamodb?.NewImage,
    old: r.dynamodb?.OldImage
  }))
}
