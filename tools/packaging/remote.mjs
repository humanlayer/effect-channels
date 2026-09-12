import assert from 'node:assert/strict'

import { assertBuiltEntry, assertOneEffect, resolvedModules } from './guard.mjs'

await import('@humanlayer/channels-delivery/protocol')
await import('@humanlayer/channels-delivery/contract')
await import('@humanlayer/channels-delivery/client')

assertBuiltEntry('@humanlayer/channels-delivery/protocol', 'protocol')
assertBuiltEntry('@humanlayer/channels-delivery/contract', 'contract')
assertBuiltEntry('@humanlayer/channels-delivery/client', 'client')
for (const forbidden of ['/DeliveryControl.js', '/Mailbox.js', '/MailboxStore.js', '/server.js', '/memory.js']) {
	assert.ok(
		![...resolvedModules].some(
			(url) => url.includes('/@humanlayer/channels-delivery/dist') && url.endsWith(forbidden),
		),
		`Contract/client import loaded private implementation module ${forbidden}`,
	)
}
assertOneEffect()
