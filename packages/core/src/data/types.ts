import { PieceLink } from '@web3-storage/data-segment'
import { UnknownLink } from '@ucanto/interface'

// Data Structures
export type Piece <Piece = PieceLink> = {
  piece: Piece
  insertedAt: number
  storefront: string
  group: string
}

export type Buffer <P> = {
  pieces: BufferedPiece<P>[]
  // identifier of storefront `did:web:web3.storage`
  storefront: string
  // identifier of group within storefront
  group: string
}

export type BufferedPiece <Piece = PieceLink> = {
  piece: Piece
  insertedAt: number
  // Policies that this piece is under
  policy: PiecePolicy
}

export type Aggregate <Piece = PieceLink, Link = UnknownLink> = {
  // PieceCid of an Aggregate `bagy...aggregate`
  piece: Piece
  // CID of dag-cbor block `bafy...cbor`
  buffer: Link
  // CID of `aggregate/add` invocation `bafy...inv`
  invocation?: Link
  // CID of `aggregate/add` task `bafy...task`
  task?: Link
  // number of milliseconds elapsed since the epoch when aggregate was submitted
  insertedAt: number
  // identifier of storefront `did:web:web3.storage`
  storefront: string
  // identifier of group within storefront
  group: string
  // known status of the aggregate (a secondary index)
  stat: AggregateStatus
}

export interface Inclusion <Piece = PieceLink> {
  // PieceCid of an Aggregate `bagy...aggregate`
  aggregate: Piece
  // PieceCid of a Filecoin Piece `bagy...content`
  piece: Piece
  // number of milliseconds elapsed since the epoch for piece inserted `1690464180271`
  insertedAt: number
  // number of milliseconds elapsed since the epoch for aggregate submission `1690464180271`
  submitedAt: number
  // number of milliseconds elapsed since the epoch for aggregate deal resolution `1690464180271`
  resolvedAt: number
  // TODO: inclusion proof?
  // status of the inclusion
  stat: InclusionStatus
  // failed reason
  failedReason?: string
}

// Enums
export type PiecePolicy =
  | NORMAL
  | RETRY

type NORMAL = 0
type RETRY = 1

export type AggregateStatus =
  | OFFERED
  | APPROVED
  | REJECTED

type OFFERED = 0
type APPROVED = 1
type REJECTED = 2

export type InclusionStatus =
  | SUCCESS
  | FAIL

type SUCCESS = 0
type FAIL = 1

// Data structure encoding/decoding

export interface Encoder <Data, MessageRecord, StoreRecord, StoreKey> {
  storeRecord: (data: Data) => Promise<StoreRecord>
  storeKey: (data: Data) => Promise<StoreKey>
  message: (data: MessageRecord) => Promise<string>
}

export interface Decoder <Data, StoreRecord, MessageRecord> {
  storeRecord: (storeRecord: StoreRecord) => Promise<Data>
  message: (message: string) => Promise<MessageRecord>
}
