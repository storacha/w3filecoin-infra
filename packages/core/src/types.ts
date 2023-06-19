import { Link } from '@ucanto/interface'
import {
  Transaction,
  UpdateResult
} from 'kysely'

import {
  Database,
  CargoState,
  FerryState
} from './sql.generated'

export interface DialectProps {
  database: string
  secretArn: string
  resourceArn: string
}

export interface CargoTable {
  /**
   * Inserts cargo to the table, with status queued to be offered in an aggregate.
   */
  insert: (cargoItem: CargoInsertInput) => Promise<Result<{}, Failure>>
  /**
   * Select cargo from the table in a given Cargo State.
   */
  selectByState: (state: CargoState, options?: GetOptions) => Promise<Result<CargoOut[], Failure>>
  /**
   * Loads cargo into an offering transaction.
   */
  updateCargoOffering: (cargoItems: Link[], ferryLink: Link) => Promise<Result<UpdateResult[], Failure>>
  /**
   * Updates state of cargo items to successfully landed in SP.
   */
  updateCargoSuccess: (ferryLink: Link) => Promise<Result<UpdateResult[], Failure>>
  /**
   * Updates state of cargo items to failed landed in SP if there was a reason, otherwise queue them again.
   * Cargo state will mutate to succeed if entire transaction succeeds.
   */
  updateCargoFailedOrQueuedOnTrx: (ferryLink: Link, failedCargoItems: FailedCargo[], trx: Transaction<Database>) => Promise<Result<{}, Failure>>
}

export interface FerryTable {
  /**
   * Inserts ferry to the table, with status queued to be arranged by a broker.
   */
  insert: (ferryItem: FerryInsertInput, cargoItems: Link[]) => Promise<Result<{}, Failure>>
  /**
   * Select ferry from the table in a given Ferry State.
   */
  selectByState: (state: FerryState, options?: GetOptions) => Promise<Result<FerryOut[], Failure>>
  /**
   * Set given ferry as being arranged by a broker.
   */
  updateFerryToArranging: (ferryItem: Link) => Promise<Result<{}, Failure>>
  /**
   * Set given ferry as successfully arranged by a broker.
   */
  updateFerryToSucceed: (ferryItem: Link) => Promise<Result<{}, Failure>>
  /**
   * Set given ferry as failed to be arranged by a broker.
   */
  updateFerryToFailed: (ferryItem: Link, failedCargoItems: FailedCargo[]) => Promise<Result<{}, Failure>>
}

export interface CargoInsertInput {
  link: Link
  size: number
  carLink: Link
  priority?: string
}

export interface CargoOut {
  link: Link
  size: number
  carLink: Link
  state: CargoState
  priority: string
  inserted?: Date
  ferryLink?: Link
  ferryFailedCode?: string
}

export interface FerryInsertInput {
  link: Link
  size: number
  priority?: string
}

export interface FerryOut {
  link: Link
  size: number
  state: FerryState
  priority: string
  inserted?: Date
}

export interface FailedCargo {
  link: Link
  code: string
}

export interface GetOptions {
  limit?: number
  orderBy?: 'priority' | 'inserted' | 'size'
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

