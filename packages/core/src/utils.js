
/**
 * @param {string} storefront
 * @param {string} group
 */
export function getMessageGroupId (storefront, group) {
  return `${storefront}:${group}`
}
