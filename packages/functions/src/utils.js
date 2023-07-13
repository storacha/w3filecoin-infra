import { RDS } from 'sst/node/rds'

/**
 * @param {string} name 
 * @returns {string}
 */
export function mustGetEnv (name) {
  const value = process.env[name]
  if (!value) throw new Error(`Missing env var: ${name}`)
  return value
}

export function getDbEnv () {
  const { defaultDatabaseName, secretArn, clusterArn} = RDS.w3filecoinrds

  return {
    database: defaultDatabaseName,
    secretArn,
    resourceArn: clusterArn,
  }
}
