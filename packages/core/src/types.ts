import { Link } from '@ucanto/interface'
import {
  UpdateResult
} from 'kysely'

export interface DialectProps {
  database: string
  secretArn: string
  resourceArn: string
}

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
  selectAll: (options?: SelectOptions) => Promise<Result<CargoOutput[], Failure>>
}

export interface ContentQueueView {
  selectAll: (options?: SelectOptions) => Promise<Result<ContentOutput[], Failure>>
}

export interface AggregateQueueView {
  selectAll: (options?: SelectOptions) => Promise<Result<AggregateOutput[], Failure>>
}

export interface DealView {
  selectAllPending: (options?: SelectOptions) => Promise<Result<DealPendingOutput[], Failure>>
  selectAllSigned: (options?: SelectOptions) => Promise<Result<DealSignedOutput[], Failure>>
  selectAllApproved: (options?: SelectOptions) => Promise<Result<DealProcessedOutput[], Failure>>
  selectAllRejected: (options?: SelectOptions) => Promise<Result<DealProcessedOutput[], Failure>>
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

export interface SelectOptions {
  limit?: number
  orderBy?: 'priority' | 'inserted' | 'size' // TODO: remove?
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

