
/**
 * @param {object} props
 * @param {import('../store/types').Store<string, Uint8Array>} props.storeClient 
 * @param {import('../queue/types').Queue<any>} props.queueClient // TODO: type piece
 * @param {string[]} props.pieceRecords 
 * @param {string} [props.groupId]
 */
export async function bufferPieces ({
  storeClient,
  queueClient,
  pieceRecords,
  groupId
}) {

}
