import { RDSData } from '@aws-sdk/client-rds-data'
import { DataApiDialect } from 'kysely-data-api'

/**
 * @param {import('../types').DialectProps} dialectOpts
 */
export function getDialect (dialectOpts) {
  return new DataApiDialect({
    mode: 'postgres',
    driver: {
      database: dialectOpts.database,
      secretArn: dialectOpts.secretArn,
      resourceArn: dialectOpts.resourceArn,
      client: new RDSData({}),
    }
  })
}
