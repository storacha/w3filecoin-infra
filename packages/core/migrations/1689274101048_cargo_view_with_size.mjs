
/**
 * @param {import('kysely').Kysely<any>} db
 */
export async function up(db) {
  await db.schema.dropView('cargo')
    .ifExists()
    .execute()

  /**
   * View for inclusion records that do not have an aggregate with piece size
   */
  await db.schema
    .createView('cargo')
    .as(
      db.selectFrom('inclusion')
        .innerJoin('piece', 'piece.link', 'inclusion.piece')
        .select([
          'inclusion.piece',
          'inclusion.aggregate',
          'inclusion.priority',
          'inclusion.inserted',
          'piece.size',
        ])
        .where('aggregate', 'is', null)
        .orderBy('priority', 'desc')
        .orderBy('inserted')
    )
    .execute()
}

/**
 * @param {import('kysely').Kysely<any>} db
 */
export async function down(db) {
  await db.schema.dropView('cargo').execute()
}
