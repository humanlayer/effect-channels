/**
 * Runs the shared store contracts against the SQL store on a real, disposable PostgreSQL: the mailbox
 * contract, the handoff and output lifecycle contract, and the output processing scenarios, the same
 * ones memory and the Durable Object run.
 *
 * Every contract test builds its store afresh, and building it empties the store's tables.
 */
import { describe } from '@effect/vitest'

import { mailboxBackendContract } from '../../delivery/test/backend-contract'
import { deliveryHandoffContract } from '../../delivery/test/delivery-handoff-contract'
import { deliveryOutputScenarios } from '../../delivery/test/delivery-output-scenarios'
import { emptyStore } from './postgres'

describe('sql store contracts', () => {
	mailboxBackendContract('sql', () => emptyStore)
	deliveryHandoffContract('sql', () => emptyStore)
	deliveryOutputScenarios('sql', () => emptyStore)
})
