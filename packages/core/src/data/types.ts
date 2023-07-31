import { PieceLink } from '@web3-storage/data-segment'
import { UnknownLink as Link } from '@ucanto/interface'

// Data Structures
export type Piece <Piece = PieceLink> = {
  piece: Piece
  insertedAt?: number
  space: string
  group: string
}

export type Buffer = {
  pieces: BufferPiece[]
}

export type BufferPiece <Piece = PieceLink> = {
  piece: Piece
  insertedAt: number
  // Policies that this piece is under
  policy: PiecePolicy
}

export type Aggregate = {
  // PieceCid of an Aggregate `bagy...aggregate`
  piece: PieceLink
  // CID of dag-cbor block `bafy...cbor`
  buffer: Link
  // CID of `aggregate/add` invocation `bafy...inv`
  invocation: Link
  // CID of `aggregate/add` task `bafy...task`
  task: Link
  // number of milliseconds elapsed since the epoch when aggregate was submitted
  insertedAt: number
  // known status of the aggregate (a secondary index)
  stat: AggregateStatus
}

export interface Inclusion {
  // PieceCid of an Aggregate `bagy...aggregate`
  aggregate: PieceLink
  // PieceCid of a Filecoin Piece `bagy...content`
  piece: PieceLink
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
  failedReaon?: string
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

type InclusionStatus =
  | SUCCESS
  | FAIL

type SUCCESS = 0
type FAIL = 1

// Data structure encoding

export interface Encoder <R> {
  storeRecord: (r: R) => Record<string, any>
  storeKey: (r: R) => Record<string, any>
  message: (r: R) => string
}

export interface Decoder <R> {
  storeRecord: (r: Record<string, any>) => R
  message: (m: string) => R
}
