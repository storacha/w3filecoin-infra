import type { ColumnType } from "kysely";

export type DealStatus = "APPROVED" | "PENDING" | "REJECTED" | "SIGNED";

export type Generated<T> = T extends ColumnType<infer S, infer I, infer U>
  ? ColumnType<S, I | undefined, U>
  : ColumnType<T, T | undefined, T>;

export type Int8 = ColumnType<string, string | number | bigint, string | number | bigint>;

export type Timestamp = ColumnType<Date, Date | string, Date | string>;

export interface Aggregate {
  link: string;
  size: Int8;
  inserted: Generated<Timestamp | null>;
}

export interface Cargo {
  piece: string | null;
  aggregate: string | null;
  priority: string | null;
  inserted: Timestamp | null;
}

export interface Content {
  link: string;
  size: Int8;
  bucket_name: string;
  bucket_endpoint: string;
  inserted: Generated<Timestamp | null>;
}

export interface ContentQueue {
  link: string | null;
  size: Int8 | null;
  bucket_name: string | null;
  bucket_endpoint: string | null;
  inserted: Timestamp | null;
}

export interface Deal {
  aggregate: string;
  status: DealStatus;
  detail: string | null;
  inserted: Generated<Timestamp | null>;
  signed: Timestamp | null;
  processed: Timestamp | null;
}

export interface DealApproved {
  aggregate: string | null;
  status: DealStatus | null;
  detail: string | null;
  inserted: Timestamp | null;
  signed: Timestamp | null;
  processed: Timestamp | null;
}

export interface DealPending {
  aggregate: string | null;
  status: DealStatus | null;
  detail: string | null;
  inserted: Timestamp | null;
  signed: Timestamp | null;
  processed: Timestamp | null;
}

export interface DealRejected {
  aggregate: string | null;
  status: DealStatus | null;
  detail: string | null;
  inserted: Timestamp | null;
  signed: Timestamp | null;
  processed: Timestamp | null;
}

export interface DealSigned {
  aggregate: string | null;
  status: DealStatus | null;
  detail: string | null;
  inserted: Timestamp | null;
  signed: Timestamp | null;
  processed: Timestamp | null;
}

export interface Inclusion {
  piece: string;
  aggregate: string | null;
  priority: string;
  inserted: Generated<Timestamp | null>;
}

export interface Piece {
  link: string;
  size: Int8;
  content: string | null;
  inserted: Generated<Timestamp | null>;
}

export interface Database {
  aggregate: Aggregate;
  cargo: Cargo;
  content: Content;
  content_queue: ContentQueue;
  deal: Deal;
  deal_approved: DealApproved;
  deal_pending: DealPending;
  deal_rejected: DealRejected;
  deal_signed: DealSigned;
  inclusion: Inclusion;
  piece: Piece;
}
