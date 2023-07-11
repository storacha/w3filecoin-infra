export const SqsSendMessageErrorName = /** @type {const} */('SqsSendMessageFailed')
export class SqsSendMessageError extends Error {
 get name() {
   return SqsSendMessageErrorName
 }
}
