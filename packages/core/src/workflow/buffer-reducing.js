/**
 * @param {object} props
 * @param {import('../store/types').Store<string, Uint8Array>} props.storeClient 
 * @param {import('../queue/types').Queue<any>} props.bufferQueueClient // TODO: type buffer
 * @param {import('../queue/types').Queue<any>} props.aggregateQueueClient // TODO: type buffer
 * @param {string[]} props.bufferRecords
 * @param {string} [props.groupId]
 */
export async function reduceBuffer ({
  storeClient,
  bufferQueueClient,
  aggregateQueueClient,
  bufferRecords,
  groupId
}) {

}
