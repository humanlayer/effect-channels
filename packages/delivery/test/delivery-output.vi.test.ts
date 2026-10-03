/**
 * Output processing over the memory store. The scenarios are shared with every store that supports handoff.
 */
import { describe } from '@effect/vitest'

import { MailboxBackendMemory } from '../src/MailboxBackendMemory'
import { deliveryOutputScenarios } from './delivery-output-scenarios'

describe('delivery output processing', () => {
	deliveryOutputScenarios('memory', () => MailboxBackendMemory)
})
