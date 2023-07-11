export const ContentFetcherErrorName = /** @type {const} */('ContentFetcherFailed')
export class ContentFetcherError extends Error {
 get name() {
   return ContentFetcherErrorName
 }
}
