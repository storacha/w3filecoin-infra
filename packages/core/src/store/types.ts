// Connectors

export interface BucketConnect {
  region: string
}

export interface TableConnect {
  region: string
}

// Store records
export type InferStoreRecord<T> = {
  [Property in keyof T]: T[Property] extends Number ? T[Property] : string
}

/** A record that is of suitable type to be put in DynamoDB. */
export type StoreRecord = Record<string, string|number>
