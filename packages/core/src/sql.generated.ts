import type { ColumnType } from "kysely";

export type CargoState = "FAILED" | "OFFERING" | "QUEUED" | "SUCCEED";

export type FerryState = "ARRANGING" | "FAILED" | "QUEUED" | "SUCCEED";

export type Generated<T> = T extends ColumnType<infer S, infer I, infer U>
  ? ColumnType<S, I | undefined, U>
  : ColumnType<T, T | undefined, T>;

export type Int8 = ColumnType<string, string | number | bigint, string | number | bigint>;

export type Timestamp = ColumnType<Date, Date | string, Date | string>;

export interface Cargo {
  link: string;
  size: Int8;
  car_link: string;
  state: CargoState;
  priority: string;
  inserted: Generated<Timestamp | null>;
  ferry_link: string | null;
  ferry_failed_code: string | null;
}

export interface Ferry {
  link: string;
  size: Int8;
  state: FerryState;
  priority: string;
  inserted: Generated<Timestamp | null>;
}

export interface Database {
  cargo: Cargo;
  ferry: Ferry;
}
