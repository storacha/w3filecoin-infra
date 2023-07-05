import type { ColumnType } from "kysely";

export type DealStatus = "APPROVED" | "PENDING" | "REJECTED" | "SIGNED";

export type Generated<T> = T extends ColumnType<infer S, infer I, infer U>
  ? ColumnType<S, I | undefined, U>
  : ColumnType<T, T | undefined, T>;

export type Int8 = ColumnType<string, string | number | bigint, string | number | bigint>;

export type Json = ColumnType<JsonValue, string, string>;

export type JsonArray = JsonValue[];

export type JsonObject = {
  [K in string]?: JsonValue;
};

export type JsonPrimitive = boolean | null | number | string;

export type JsonValue = JsonArray | JsonObject | JsonPrimitive;

export type Timestamp = ColumnType<Date, Date | string, Date | string>;

export interface Aggregate {
  link: string;
  size: Int8;
  inserted: Generated<Timestamp | null>;
}

export interface AggregateQueue {
  link: string | null;
  size: Int8 | null;
  inserted: Timestamp | null;
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
  source: Json;
  inserted: Generated<Timestamp | null>;
}

export interface ContentQueue {
  link: string | null;
  size: Int8 | null;
  source: Json | null;
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
  aggregate_queue: AggregateQueue;
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
