import { describe } from '@effect/vitest'

import { MailboxBackendMemory } from '../src/MailboxBackendMemory'
import { mailboxBackendContract } from './backend-contract'
import { deliveryHandoffContract } from './delivery-handoff-contract'

describe('mailbox backend contract', () => {
	mailboxBackendContract('memory', () => MailboxBackendMemory)
	deliveryHandoffContract('memory', () => MailboxBackendMemory)
})
