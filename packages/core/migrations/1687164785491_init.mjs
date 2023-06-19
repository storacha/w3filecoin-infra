import { sql } from 'kysely'


/**
 * @param {import('kysely').Kysely<any>} db
 */
export async function up(db) {
  await db.schema
    .createType('cargo_state')
    .asEnum(['QUEUED', 'OFFERING', 'SUCCEED', 'FAILED'])
    .execute()

  await db.schema
    .createType('ferry_state')
    .asEnum(['QUEUED', 'ARRANGING', 'SUCCEED', 'FAILED'])
    .execute()

  await db.schema
    .createTable('ferry')
    .addColumn('link', 'text', (col) => col.primaryKey())
    .addColumn('size', 'bigint', (col) => col.notNull())
    .addColumn('state', sql`ferry_state`, (col) => col.notNull())
    .addColumn('priority', 'text', (col) => col.notNull())
    .addColumn('inserted', 'timestamp', (col) => col.defaultTo('now()'))
    .execute()

  await db.schema
    .createIndex('ferry_stat_idx')
    .on('ferry')
    .column('state')
    .execute()

  await db.schema
    .createTable('cargo')
    .addColumn('link', 'text', (col) => col.primaryKey())
    .addColumn('size', 'bigint', (col) => col.notNull())
    .addColumn('car_link', 'text', (col) => col.notNull())
    .addColumn('state', sql`cargo_state`, (col) => col.notNull())
    .addColumn('priority', 'text', (col) => col.notNull())
    .addColumn('inserted', 'timestamp', (col) => col.defaultTo('now()'))
    .addColumn('ferry_link', 'text', (col) => col.references('ferry.link'))
    .addColumn('ferry_failed_code', 'text')
    .execute()

  await db.schema
    .createIndex('cargo_stat_idx')
    .on('cargo')
    .column('state')
    .execute()

  await db.schema
    .createIndex('cargo_car_link_idx')
    .on('cargo')
    .column('car_link')
    .execute()

  await db.schema
    .createIndex('cargo_aggregate_link_idx')
    .on('cargo')
    .column('ferry_link')
    .execute()
}

/**
 * @param {import('kysely').Kysely<any>} db
 */
export async function down(db) {
  // Cargo Indexes
  await db.schema.dropIndex('cargo_aggregate_link_idx').execute()
  await db.schema.dropIndex('cargo_car_link_idx').execute()
  await db.schema.dropIndex('cargo_stat_idx').execute()

  // Cargo Table
  await db.schema.dropTable('cargo').execute()

  // Ferry Indexes
  await db.schema.dropIndex('ferry_stat_idx').execute()

  // Ferry Table
  await db.schema.dropTable('ferry').execute()

  // Types
  await db.schema.dropType('ferry_state').execute()
  await db.schema.dropType('cargo_state').execute()
}
