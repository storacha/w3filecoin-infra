// Store Record

// Connectors

export interface BucketConnect {
  region: string
}

export interface TableConnect {
  region: string
}

// Store records

export interface DealStoreRecord {
  // PieceCid of an Aggregate `bagy...aggregate`
  piece: string
  // address of the Filecoin storage provider storing deal
  provider: string
  // deal identifier
  dealId: number
  // epoch of deal expiration
  expirationEpoch: number
  // source of the deal information
  source: string
  // Date when deal was added as ISO string
  insertedAt: string
}

export interface DealStoreRecordKey extends Pick<DealStoreRecord, 'piece' | 'dealId'> {}
export interface DealStoreRecordQueryByPiece extends Pick<DealStoreRecord, 'piece'> {}