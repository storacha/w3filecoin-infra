export const ContentResolverErrorName = /** @type {const} */('ContentResolverFailed')
export class ContentResolverError extends Error {
 get name() {
   return ContentResolverErrorName
 }
}
