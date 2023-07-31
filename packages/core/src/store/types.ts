import { Store } from '@web3-storage/filecoin-api/types'

import { Result } from '../types'

// Store
export interface ExtendedStore <Record, Key> extends Store<Record> {
  /**
   * Gets content data from the store.
   */
  get(key: Key): Promise<Result<Record, StoreGetError>>
}

// Errors
export interface StoreGetError extends Error {
  // TODO
}

// Connectors

export interface BucketConnect {
  region: string
}

export interface TableConnect {
  region: string
}
