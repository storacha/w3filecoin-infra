import { test } from './helpers/context.js'

import git from 'git-rev-sync'

import {
  getApiEndpoint,
  getStage,
} from './helpers/deployment.js'

test.before(t => {
  t.context = {
    apiEndpoint: getApiEndpoint(),
  }
})

test('GET /version', async t => {
  const stage = getStage()
  const response = await fetch(`${t.context.apiEndpoint}/version`)
  t.is(response.status, 200)

  const body = await response.json()
  t.is(body.env, stage)
  t.is(body.commit, git.long('.'))
})
