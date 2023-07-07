import { Link } from '@ucanto/interface'
import {
  UpdateResult,
  Kysely
} from 'kysely'
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
  put(items: Item[]): Promise<Result<{}, Failure>>
}
export interface Consumer<Item> {
  /**
   * Peek items of the queue without removing them.
   */
  peek(options?: ConsumerOptions): Promise<Result<Item[], Failure>>
}

export interface PriorityProducer<Item extends ItemWithPriority> extends Producer<Item>{}
export interface ItemWithPriority {
  priority?: number
}
export interface Queue<In, Out = Inserted<In>> extends Producer<In>, Consumer<Out> {}
export interface PriorityQueue<In extends ItemWithPriority, Out = Inserted<In>> extends PriorityProducer<In>, Consumer<Out> {}
export type Inserted<In> = In & { inserted: string }

/**
 * Content Queue
 */
export interface Content {
  link: Link
  size: number
  source: ContentSource[]
}
export type ContentQueue = Queue<Content>

/**
 * Piece Queue
 */
export interface Piece {
  link: Link
  content: Link
  size: number
  priority?: number
}
export interface Inclusion {
  piece: Link
  priority: number
}
export type PieceQueue = PriorityQueue<Piece, Inserted<Inclusion>>

/**
 * Aggregate Queue
 */
export interface Aggregate {
  link: Link
  size: number
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

// ------

export interface ContentTable {
  /**
   * Inserts content data to the table, so that it gets processed and into a deal.
   */
  insert: (item: ContentInsertInput) => Promise<Result<{}, Failure>>
}

export interface PieceTable {
  /**
   * Inserts piece data to the table, so that it gets aggregated and into a deal.
   */
  insert: (item: PieceInsertInput, contentLink: Link) => Promise<Result<{}, Failure>>
}

export interface AggregateTable {
  /**
   * Inserts aggregate data to the table, so that it gets into a deal.
   */
  insert: (aggregate: AggregateInsertInput, pieces: Link[]) => Promise<Result<{}, Failure>>
}

export interface InclusionTable {
  /**
   * Inserts inclusion data to the table, so that inserted piece is aggregated.
   */
  insert: (item: InclusionInsertInput) => Promise<Result<{}, Failure>>
  aggregate: (items: Link[], aggregateLink: Link) => Promise<Result<UpdateResult[], Failure>>
}

export interface DealTable {
  /**
   * Inserts deal data to the table, so that deal flow can be tracked.
   */
  insert: (item: DealInsertInput) => Promise<Result<{}, Failure>>
}

export interface CargoView {
  selectAll: (options?: ConsumerOptions) => Promise<Result<CargoOutput[], Failure>>
}

export interface ContentQueueView {
  selectAll: (options?: ConsumerOptions) => Promise<Result<ContentOutput[], Failure>>
}

export interface AggregateQueueView {
  selectAll: (options?: ConsumerOptions) => Promise<Result<AggregateOutput[], Failure>>
}

export interface DealView {
  selectAllPending: (options?: ConsumerOptions) => Promise<Result<DealPendingOutput[], Failure>>
  selectAllSigned: (options?: ConsumerOptions) => Promise<Result<DealSignedOutput[], Failure>>
  selectAllApproved: (options?: ConsumerOptions) => Promise<Result<DealProcessedOutput[], Failure>>
  selectAllRejected: (options?: ConsumerOptions) => Promise<Result<DealProcessedOutput[], Failure>>
}

export interface ContentInsertInput {
  link: Link
  size: number
  source: ContentSource[]
}

export interface ContentOutput extends ContentInsertInput {
  inserted: string
}

export type ContentSource = {
  bucketName: string
  bucketRegion: string
  key: string
  bucketUrl?: string
}

export interface PieceInsertInput {
  link: Link
  size: number
}

export interface AggregateInsertInput {
  link: Link
  size: number
}

export interface AggregateOutput extends AggregateInsertInput {
  inserted: string
}

export interface InclusionInsertInput {
  piece: Link
  priority?: number
}

export interface CargoOutput extends InclusionInsertInput {
  inserted: string
}

export interface DealInsertInput {
  aggregate: Link
}

export interface DealPendingOutput extends DealInsertInput {
  inserted: string
}

export interface DealSignedOutput extends DealInsertInput {
  signed: string
}

export interface DealProcessedOutput extends DealInsertInput {
  processed: string
}

export interface ConsumerOptions {
  limit?: number
}

export type Result<T = unknown, X extends {} = {}> = Variant<{
  ok: T
  error: X
}>

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

