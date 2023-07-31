/**
 * @param {object} props
 * @param {import('../store/types').Store<string, Uint8Array>} props.bufferStoreClient
 * @param {import('../store/types').Store<any, undefined>} props.aggregateStoreClient // TODO: Type
 * @param {string} props.aggregateRecord 
 * @param {string} [props.groupId]
 */
export async function addAggregate ({
  bufferStoreClient,
  aggregateStoreClient,
  aggregateRecord,
  groupId
}) {

}
