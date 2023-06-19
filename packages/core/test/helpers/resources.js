import { promises as fs } from 'fs'
import path from 'path'
import { PostgreSqlContainer } from 'testcontainers'
import { customAlphabet } from 'nanoid'

import Pool from 'pg-pool'
import { Kysely, PostgresDialect, Migrator, FileMigrationProvider } from 'kysely'

export async function createDatabase () {
  const database = (customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10))()
  const container = await new PostgreSqlContainer().withDatabase(database).start()

  const client = new Kysely({
    dialect: new PostgresDialect({
      pool: new Pool({
        host: container.getHost(),
        port: container.getPort(),
        database: container.getDatabase(),
        user: container.getUsername(),
        password: container.getPassword()
      })
    })
  })

  // Perform migrations
  const migrator = new Migrator({
    db: client,
    provider: new FileMigrationProvider({
      fs,
      path,
      // Path to the folder that contains all your migrations.
      migrationFolder: `${process.cwd()}/migrations`
    })
  })

  await migrator.migrateToLatest()

  return {
    client
  }
}
