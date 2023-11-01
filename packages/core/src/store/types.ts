import { Store } from '@web3-storage/filecoin-api/types'
import { ByteView } from '@ucanto/interface'
import { Contract } from '../deal-tracker/types'
import { Link } from 'multiformats'
import { InclusionProof } from '@web3-storage/capabilities/types'

// Connectors

export interface BucketConnect {
  region: string
}

export interface TableConnect {
  region: string
}

// Stores
export interface BucketStoreRecord {
  key: string
  body: any
}

// Store records
export type InferStoreRecord<T> = {
  [Property in keyof T]: T[Property] extends Number ? T[Property] : string
}

/** A record that is of suitable type to be put in DynamoDB. */
export type StoreRecord = Record<string, string|number>

/** ---------------------- Aggregator ---------------------- */
/**
 * Custom PieceStoreRecord given we need to change stat.
 */
export type AggregatorPieceStoreRecord = {
  // Piece CID for the content.
  piece: string
  // Grouping information for submitted piece.
  group: string
  /**
   * Status of the offered piece.
   * - offered = acknowledged received for aggregation.
   * - accepted = accepted into an aggregate and offered for inclusion in filecoin deal(s).
   */
  stat: AggregatorPieceStoreRecordStatus
  /**
   * Insertion date ISO string.
   */
  insertedAt: string
  /**
   * Update date ISO string.
   */
  updatedAt: string
}

export enum AggregatorPieceStoreRecordStatus {
  Offered = 0,
  Accepted = 1,
}

/**
 * Custom InclusionStoreRecord given we need to store inclusion proof separately.
 */

export interface AggregatorInclusionRecord extends Omit<import('@web3-storage/filecoin-api/aggregator/api').InclusionRecord, 'inclusion'> {
  inclusion: Link
}

export interface AggregatorInclusionStoreRecord {
  /**
   * Piece CID for the content.
   */
  piece: string
  /**
   * Piece CID of an aggregate.
   */
  aggregate: string
  /**
   * Grouping information for submitted piece.
   */
  group: string
  /**
   * dag-cbor string with inclusion proof cid.
   */
  inclusion: string
  /**
   * Insertion date ISO string.
   */
  insertedAt: string
}

export type InclusionProofStore = Store<Link, InclusionProofRecord>

export interface InclusionProofRecord {
  /**
   * Inclusion proof record
   */
  inclusion: InclusionProof
  /**
   * `bafy...cbor` as CID of dag-cbor block with inclusion proof.
   */
  block: Link
}



/** ---------------------- Dealer ---------------------- */
/**
 * Custom Dealer Aggregate store record given we need to collapse Deal Metadata into different columns
 * together with status change to stat.
 */
export type DealerAggregateStoreRecord = {
  // PieceCid of an Aggregate `bagy...aggregate`
  aggregate: string
  // Encoded block CID of list of pieces in an aggregate.
  pieces: string
  // Deal identifier on the Filecoin chain
  dealMetadataDealId?: number
  // Deal data type (always 0 for now)
  dealMetadataDataType?: number
  // Status of the offered 
  stat: DealerAggregateStoreRecordStatus
  // Date when aggregate was added as ISO string
  insertedAt: string
  // Date when aggregate information was updated as ISO string
  updatedAt: string
}

export enum DealerAggregateStoreRecordStatus {
  Offered = 0,
  Accepted = 1,
  Invalid = 2
}

export interface DealerAggregateStoreRecordKey extends Pick<DealerAggregateStoreRecord, 'aggregate' | 'dealMetadataDealId'> {}
export interface DealerAggregateStoreRecordQueryByStatus extends Pick<DealerAggregateStoreRecord, 'stat'> {}
export interface DealerAggregateStoreRecordQueryByAggregate extends Pick<DealerAggregateStoreRecord, 'aggregate'> {}

export type DealerOfferStoreRecord = {
  key: string
  value: DealerOfferStoreRecordValue
}

export interface DealerOfferStoreRecordValue {
  // PieceCid of an Aggregate `bagy...aggregate`
  aggregate: string
  // PieceCid of sorted pieces used to generate aggregate
  pieces: string[]
  // Issuer of the collection of aggregates
  collection: string
  // Order that broker will rely on to prioritize deals.
  orderID: number
}

/** ---------------------- Deal Tracker ---------------------- */
export interface DealArchiveRecord {
  key: string
  value: ByteView<{
    [k: string]: Contract[];
  }>
}

export type DealArchiveStore = Store<string, DealArchiveRecord>
