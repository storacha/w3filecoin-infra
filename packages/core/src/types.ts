import { UnknownLink as Link } from 'multiformats/link'
import { PaddedPieceSize } from '@web3-storage/data-segment'
import { Kysely } from 'kysely'
import { Database } from './schema'

export interface DialectProps {
  database: string
  secretArn: string
  resourceArn: string
}

export type DatabaseConnect = Kysely<Database> | DialectProps

export interface Producer<Item> {
  /**
   * Puts content data to the queue, so that it gets processed.
   */
  put(item: Item): Promise<Result<{}, PutError>>
}
export interface Consumer<Item> {
  /**
   * Peek items of the queue without removing them.
   */
  peek(options?: ConsumerOptions): Promise<Result<Item[], PeerError>>
}

export interface PriorityProducer<Item extends ItemWithPriority> extends Producer<Item>{}
export interface ItemWithPriority {
  priority?: number
}
export interface ConsumerOptions {
  limit?: number
  offset?: number
}
export interface Queue<In, Out = Inserted<In>> extends Producer<In>, Consumer<Out> {}
export interface PriorityQueue<In extends ItemWithPriority, Out = Inserted<In>> extends PriorityProducer<In>, Consumer<Out> {}
export type Inserted<In> = In & { inserted: string }

/**
 * Errors
 */
export type PutError =
  | DatabaseOperationError
  | DatabaseForeignKeyConstraintError
  | DatabaseValueToUpdateAlreadyTakenError
  | ContentEncodeError

export type PeerError = DatabaseOperationError

/**
 * Content Queue
 */
export interface Content {
  link: Link
  size: number
  source: URL[]
}
export type ContentQueue = Queue<Content>

/**
 * Piece Queue
 */
export interface Piece {
  link: Link
  content: Link
  size: PaddedPieceSize
  priority?: number
}
export interface Inclusion {
  piece: Link
  priority: number
  aggregate?: Link
  size: PaddedPieceSize
}
export type PieceQueue = PriorityQueue<Piece, Inserted<Inclusion>>

/**
 * Aggregate Queue
 */
export interface Aggregate {
  link: Link
  size: PaddedPieceSize
}
export interface AggregateWithInclusionPieces extends Aggregate {
  pieces: Link[]
}

export type AggregateQueue = Queue<AggregateWithInclusionPieces, Inserted<Aggregate>>

/**
 * Deal Queue
 */
export interface Deal {
  aggregate: Link
}
export type DealQueue = Queue<Deal>

export type ContentSource = {
  provider: 'r2' | 's3'
  bucketName: string
  bucketRegion: string
  key: string
}

export interface ContentResolver {
  resolve: (item: Content) => Promise<Result<Uint8Array, ContentResolverError>>
}

export type Result<T = unknown, X extends {} = {}> = Variant<{
  ok: T
  error: X
}>

/**
 * Workflows
 */

export type ConsumerWorkflowResponse = Promise<Result<ConsumerWorkflowOkResponse, ConsumerWorkflowErrorResponse>>
export type ProducerWorkflowResponse = Promise<Result<{}, ProducerWorkflowErrorResponse>>
export type AggregatorWorkflowResponse = Promise<Result<ConsumerWorkflowOkResponse, AggregatorWorkflowErrorResponse>>

export interface ConsumerWorkflowOkResponse {
  count: number
}

export type ConsumerWorkflowErrorResponse =
  | DatabaseOperationError
  | SqsSendMessageError

export type ProducerWorkflowErrorResponse =
  | DatabaseOperationError
  | ContentResolverError
  | ContentEncodeError
  | DatabaseForeignKeyConstraintError
  | DatabaseValueToUpdateAlreadyTakenError

export type AggregatorWorkflowErrorResponse =
  | ContentEncodeError
  | DatabaseOperationError
  | DatabaseForeignKeyConstraintError
  | DatabaseValueToUpdateAlreadyTakenError

/**
 * Errors
 */

export interface DatabaseOperationError extends Error {
  name: 'DatabaseOperationFailed'
}
export interface DatabaseForeignKeyConstraintError extends Error {
  name: 'DatabaseForeignKeyConstraint'
}

export interface DatabaseValueToUpdateAlreadyTakenError extends Error {
  name: 'DatabaseValueToUpdateAlreadyTaken'
}

export interface SqsSendMessageError extends Error {
  name: 'SqsSendMessageFailed'
}

export interface ContentResolverError extends Error {
  name: 'ContentResolverFailed'
}

export interface ContentEncodeError extends Error {
  name: 'ContentEncodeFailed'
}

/**
 * Utility type for defining a [keyed union] type as in IPLD Schema. In practice
 * this just works around typescript limitation that requires discriminant field
 * on all variants.
 *
 * ```ts
 * type Result<T, X> =
 *   | { ok: T }
 *   | { error: X }
 *
 * const demo = (result: Result<string, Error>) => {
 *   if (result.ok) {
 *   //  ^^^^^^^^^ Property 'ok' does not exist on type '{ error: Error; }`
 *   }
 * }
 * ```
 *
 * Using `Variant` type we can define same union type that works as expected:
 *
 * ```ts
 * type Result<T, X> = Variant<{
 *   ok: T
 *   error: X
 * }>
 *
 * const demo = (result: Result<string, Error>) => {
 *   if (result.ok) {
 *     result.ok.toUpperCase()
 *   }
 * }
 * ```
 *
 * [keyed union]:https://ipld.io/docs/schemas/features/representation-strategies/#union-keyed-representation
 */
export type Variant<U extends Record<string, unknown>> = {
  [Key in keyof U]: { [K in Exclude<keyof U, Key>]?: never } & {
    [K in Key]: U[Key]
  }
}[keyof U]

export interface Failure extends Error {}

