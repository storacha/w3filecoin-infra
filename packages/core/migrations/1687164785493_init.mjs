import { sql } from 'kysely'


/**
 * @param {import('kysely').Kysely<any>} db
 */
export async function up(db) {
  await db.schema
    .createType('deal_status')
    .asEnum(['PENDING', 'SIGNED', 'APPROVED', 'REJECTED'])
    .execute()

  /**
   * Table describes queue of verified CARs to be stored in filecoin
   * CAR is considered in queue when there is no piece referencing it.
   */
  await db.schema
    .createTable('content')
    .addColumn('link', 'text', (col) => col.primaryKey())
    .addColumn('size', 'bigint', (col) => col.notNull())
    .addColumn('bucket_name', 'text', (col) => col.notNull())
    .addColumn('bucket_endpoint', 'text', (col) => col.notNull())
    .addColumn('inserted', 'timestamp', (col) => col.defaultTo('now()'))
    .execute()
  
  /**
   * Table describes pieces derived corresponding to CARs in the content table. Link (commP) is
   * unique even though cargo reference is not, that is because there may be an error in piece
   * derivation and in that case another correct piece will reference the same content.
   */
  await db.schema
    .createTable('piece')
    .addColumn('link', 'text', (col) => col.primaryKey())
    .addColumn('size', 'bigint', (col) => col.notNull())
    .addColumn('content', 'text', (col) => col.references('content.link'))
    .addColumn('inserted', 'timestamp', (col) => col.defaultTo('now()'))
    .execute()
  
  await db.schema
    .createIndex('piece_content_idx')
    .on('piece')
    .column('content')
    .execute()
  await db.schema
    .createIndex('piece_inserted_idx')
    .on('piece')
    .column('inserted')
    .execute()
  
  /**
   * Content for which we need to derive piece CIDs. We will have a process that
   * reads from this queue and writes into `piece` table.
   */
  await db.schema
    .createView('content_queue')
    .as(
      db.selectFrom('content')
        .leftJoin('piece', 'content.link', 'piece.content')
        .where('piece.content', 'is', null)
        .select([
          'content.link',
          'content.size',
          'content.bucket_name',
          'content.bucket_endpoint',
          'content.inserted',
        ])
        .orderBy('piece.inserted')
    )
    .execute()
  
  /**
   * Table for created aggregates.
   */
  await db.schema
    .createTable('aggregate')
    .addColumn('link', 'text', (col) => col.primaryKey())
    .addColumn('size', 'bigint', (col) => col.notNull())
    .addColumn('inserted', 'timestamp', (col) => col.defaultTo('now()'))
    .execute()

  /**
   * Table describing pieces to be included into aggregates. If aggregate is NULL then the
   * piece is queued for the aggregation.
   */
  await db.schema
    .createTable('inclusion')
    .addColumn('piece', 'text', (col) => col.references('piece.link').notNull())
    .addColumn('aggregate', 'text', (col) => col.defaultTo(null))
    .addColumn('priority', 'text', (col) => col.notNull())
    .addColumn('inserted', 'timestamp', (col) => col.defaultTo('now()'))
    .addUniqueConstraint('piece_aggregate_unique', ['aggregate', 'piece'])
    .addForeignKeyConstraint(
      'aggregate_id_foreign',
      ['aggregate'],
      'aggregate',
      ['link']
    )
    .execute()

  await db.schema
    .createIndex('inclusion_inserted_idx')
    .on('inclusion')
    .column('inserted')
    .execute()

  await db.schema
    .createIndex('piece_aggregate_unique_idx')
    .on('inclusion')
    .columns(['aggregate', 'piece'])
    .execute()

  /**
   * View for inclusion records that do not have an aggregate.
   */
  await db.schema
    .createView('cargo')
    .as(
      db.selectFrom('inclusion')
        .selectAll()
        .where('aggregate', 'is', null)
        .orderBy('inserted')
    )
    .execute()

  /**
   * State of aggregate deals. When aggregate is sent to spade-proxy status is 'PENDING'.
   * When spade-proxy requests a wallet signature, status will be updated to 'SIGNED'. Once
   * deal is accepted status changes to `APPROVED`, but if deal fails status will be set to
   * `REJECTED`.
  */
  await db.schema
    .createTable('deal')
    .addColumn('aggregate', 'text', (col) => col.references('aggregate.link').primaryKey())
    .addColumn('status', sql`deal_status`, (col) => col.notNull())
    .addColumn('detail', 'text')
    .addColumn('inserted', 'timestamp', (col) => col.defaultTo('now()'))
    .addColumn('signed', 'timestamp')
    .addColumn('processed', 'timestamp')
    .execute()

  await db.schema
    .createIndex('deal_inserted_idx')
    .on('deal')
    .column('inserted')
    .execute()
  await db.schema
    .createIndex('deal_status_idx')
    .on('deal')
    .column('status')
    .execute()

  /**
   * View for pending deals waiting signining request
   */
  await db.schema
    .createView('deal_pending')
    .as(
      db.selectFrom('deal')
        .selectAll()
        .where('status', '=', 'PENDING')
        .orderBy('inserted')
    )
    .execute()

  /**
   * View for pending deals already signed
   */
  await db.schema
    .createView('deal_signed')
    .as(
      db.selectFrom('deal')
        .selectAll()
        .where('status', '=', 'SIGNED')
        .orderBy('signed')
    )
    .execute()

  /**
   * View for approved deals 
   */
  await db.schema
    .createView('deal_approved')
    .as(
      db.selectFrom('deal')
        .selectAll()
        .where('status', '=', 'APPROVED')
        .orderBy('processed')
    )
    .execute()

  /**
   * View for rejected deals
   */
  await db.schema
    .createView('deal_rejected')
    .as(
      db.selectFrom('deal')
        .selectAll()
        .where('status', '=', 'REJECTED')
        .orderBy('processed')
    )
    .execute()
}

/**
 * @param {import('kysely').Kysely<any>} db
 */
export async function down(db) {
  // Indexes
  await db.schema.dropIndex('piece_inserted_idx').execute()
  await db.schema.dropIndex('piece_content_idx').execute()
  await db.schema.dropIndex('inclusion_inserted_idx').execute()

  // Views
  await db.schema.dropView('deal_pending').execute()
  await db.schema.dropView('content_queue').execute()
  await db.schema.dropView('cargo').execute()

  // Tables
  await db.schema.dropTable('piece').execute()
  await db.schema.dropTable('content').execute()
  await db.schema.dropTable('inclusion').execute()
  await db.schema.dropTable('aggregate').execute()
  await db.schema.dropTable('deal').execute()

  // Types
  await db.schema.dropType('deal_status').execute()
}
