import { fetch } from '@web-std/fetch'

import { test } from './helpers/context.js'
import { getRedisApiEndpoint } from './helpers/deployment.js'

import { AGGREGATE_KEY } from '../stacks/config.js'

test('can get current aggregate id', async t => {
  const request = await fetch(getRedisApiEndpoint())

  const response = await request.json()
  const id = response[AGGREGATE_KEY]

  t.truthy(id)
  // Aggregate id smaller than current date
  t.truthy(Number(id) < Date.now())
})
