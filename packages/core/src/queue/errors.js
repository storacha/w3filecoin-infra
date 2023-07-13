export const ContentEncodeErrorName = /** @type {const} */('ContentEncodeFailed')
export class ContentEncodeError extends Error {
 get name() {
   return ContentEncodeErrorName
 }
}