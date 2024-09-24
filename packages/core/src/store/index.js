import { S3Client } from '@aws-sdk/client-s3'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'

/** @type {Record<string, import('@aws-sdk/client-s3').S3Client>} */
const s3Clients = {}

/**
 * @param {import('./types.js').BucketConnect | S3Client} target 
 */
export function connectBucket (target) {
  if (target instanceof S3Client) {
    return target
  }
  if (!s3Clients[target.region]) {
    s3Clients[target.region] = new S3Client(target)
  }
  return s3Clients[target.region]
}

/** @type {Record<string, import('@aws-sdk/client-dynamodb').DynamoDBClient>} */
const dynamoClients = {}

/**
 * @param {import('./types.js').TableConnect | DynamoDBClient} target 
 */
export function connectTable (target) {
  if (target instanceof DynamoDBClient) {
    return target
  }
  if (!dynamoClients[target.region]) {
    dynamoClients[target.region] = new DynamoDBClient(target)
  }
  return dynamoClients[target.region]
}

/** @typedef {import('sst/constructs').TableProps} TableProps */

/** ------------------- Aggregator ------------------- */

/** @type TableProps */
export const aggregatorPieceStoreTableProps = {
  fields: {
    piece: 'string',        // `bagy...content` as PieceCid of a Filecoin Piece
    group: 'string',        // `did:web:free.web3.storage`
    stat: 'number',         // `0` as 'OFFERED' | `1` as 'ACCEPTED'
    insertedAt: 'string',   // Insertion date as ISO string
    updatedAt: 'string',    // Updated date as ISO string
  },
  // piece + group must be unique to satisfy index constraint
  primaryIndex: { partitionKey: 'piece', sortKey: 'group' },
}

/** @type TableProps */
export const aggregatorAggregateStoreTableProps = {
  fields: {
    aggregate: 'string',    // `bagy...aggregate` as PieceCid of an Aggregate (primary index, partition key)
    buffer: 'string',       // `bafy...cbor` as CID of dag-cbor of the buffer storing aggregate pieces.
    pieces: 'string',       // `bafy...cbor` as CID of dag-cbor block with list of pieces in an aggregate.
    group: 'string',        // `did:web:free.web3.storage`
    insertedAt: 'string',   // Insertion date as ISO string
  },
  // piece must be unique to satisfy index constraint
  primaryIndex: { partitionKey: 'aggregate' },
  globalIndexes: {
    group: {
      partitionKey: 'group',
      projection: 'all'
    }
  }
}

/** @type TableProps */
export const aggregatorInclusionStoreTableProps = {
  fields: {
    aggregate: 'string',    // `bagy...aggregate` as PieceCid of an Aggregate
    piece: 'string',        // `bagy...content` as PieceCid of a Filecoin Piece
    group: 'string',        // `did:web:free.web3.storage`
    inclusion: 'string',    // proof that the piece is included in the aggregate
    insertedAt: 'string',   // Insertion date as ISO string
  },
  // aggregate + piece must be unique to satisfy index constraint
  primaryIndex: { partitionKey: 'aggregate', sortKey: 'piece' },
  globalIndexes: {
    indexPiece: {
      partitionKey: 'piece',
      sortKey: 'group',
      projection: 'all'
    }
  }
}

/** ------------------- Dealer ------------------- */

/** @type TableProps */
export const dealerAggregateStoreTableProps = {
  fields: {
    aggregate: 'string',            // `bagy...aggregate` as PieceCid of an Aggregate (primary index, partition key)
    pieces: 'string',               // `bafy...cbor` as CID of dag-cbor block
    stat: 'number',                 // `0` as 'OFFERED' | `1` as 'APPROVED' | `2` as 'REJECTED'
    insertedAt: 'string',           // Insertion date as ISO string
    updatedAt: 'string',            // Updated date as ISO string
  },
  // aggregate+dealMetadataDealId must be unique to satisfy index constraint
  primaryIndex: { partitionKey: 'aggregate' },
  globalIndexes: {
    stat: {
      partitionKey: 'stat',
      sortKey: 'insertedAt',
      projection: 'all'
    },
  }
}

/** ------------------- Deal Tracker ------------------- */

/** @type TableProps */
export const dealStoreTableProps = {
  fields: {
    piece: 'string',            // `bagy...aggregate` as PieceCid of an Aggregate (primary index, partition key)
    provider: 'string',         // `f020378` address of the Filecoin storage provider storing deal
    dealId: 'number',           // '111' deal identifier
    expirationEpoch: 'number',  // '4482396' epoch of deal expiration
    source: 'string',           // 'cargo.dag.haus' source of the deal information
    insertedAt: 'string',       // Insertion date as ISO string
    updatedAt: 'string',       // Update date as ISO string
  },
  primaryIndex: { partitionKey: 'piece', sortKey: 'dealId' },
  globalIndexes: {
    piece: {
      partitionKey: 'piece',
      projection: 'all'
    }
  }
}
