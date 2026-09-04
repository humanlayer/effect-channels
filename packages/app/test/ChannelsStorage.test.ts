import { assert, it } from '@effect/vitest'
import { Predicate } from 'effect'

import { ChannelsStorage, createChannelsApp, postgres, slack } from '../src/index.ts'

it('accepts opaque memory and Postgres storage configurations', () => {
	const memoryApp = createChannelsApp({ providers: [slack()], storage: ChannelsStorage.memory() })
	const postgresApp = createChannelsApp({ providers: [slack()], storage: ChannelsStorage.postgres() })
	const compatibilityApp = createChannelsApp({ providers: [slack()], storage: postgres() })
	assert.ok(Predicate.isFunction(memoryApp.handle))
	assert.ok(memoryApp.routes)
	assert.ok(postgresApp.routes)
	assert.ok(compatibilityApp.routes)
})
