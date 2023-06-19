import { createRequire } from 'module'
import fs from 'fs'
import path from 'path'

export function getStage () {
  const stage = process.env.SST_STAGE || process.env.SEED_STAGE_NAME
  if (stage) {
    return stage
  }

  const f = fs.readFileSync(path.join(
    process.cwd(),
    '.sst/stage'
  ))

  return f.toString()
}

export const getStackName = () => {
  const stage = getStage()
  return `${stage}-w3filecoin`
}

export const getApiEndpoint = () => {
  const stage = getStage()

  // CI/CD deployment
  if (process.env.SEED_APP_NAME) {
    return `https://${stage}.filecoin.web3.storage`
  }

  const require = createRequire(import.meta.url)
  const testEnv = require(path.join(
    process.cwd(),
    '.sst/outputs.json'
  ))

  // Get Upload API endpoint
  const id = 'ApiStack'
  return testEnv[`${getStackName()}-${id}`].ApiEndpoint
}