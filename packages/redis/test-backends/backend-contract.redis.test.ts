/**
 * Runs the shared store contracts against the Redis store on a real, disposable Redis: the mailbox
 * contract, the handoff and output lifecycle contract, and the output processing scenarios, the same
 * ones memory, the Durable Object, and Postgres run.
 *
 * Every contract test builds its store afresh, and building it empties the database.
 */
import { describe } from '@effect/vitest'

import { mailboxBackendContract } from '../../delivery/test/backend-contract'
import { deliveryHandoffContract } from '../../delivery/test/delivery-handoff-contract'
import { deliveryOutputScenarios } from '../../delivery/test/delivery-output-scenarios'
import { emptyStore } from './redis'

describe('redis store contracts', () => {
	mailboxBackendContract('redis', () => emptyStore)
	deliveryHandoffContract('redis', () => emptyStore)
	deliveryOutputScenarios('redis', () => emptyStore)
})
