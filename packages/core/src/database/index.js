import { Kysely } from 'kysely'
import { RDSData } from '@aws-sdk/client-rds-data'
import { DataApiDialect } from 'kysely-data-api'

/**
 * @param {import('../types').DatabaseConnect} target
 * @returns {Kysely<import('../schema').Database>}
 */
export const connect = (target) => {
  if (target instanceof Kysely) {
    return target
  } else {
    const dialect = new DataApiDialect({
      mode: 'postgres',
      driver: {
        database: target.database,
        secretArn: target.secretArn,
        resourceArn: target.resourceArn,
        client: new RDSData({}),
      }
    })
    return new Kysely({
      dialect
    })
  }
}

