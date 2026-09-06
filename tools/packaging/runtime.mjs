import assert from 'node:assert/strict'

import * as Delivery from '@humanlayer/channels-delivery'
import * as Slack from '@humanlayer/channels-slack'
import { Effect } from 'effect'

import { deliveryIdentity, slackIdentity } from './compiled/consumer.js'
import { assertBuiltEntry, assertOneEffect } from './guard.mjs'

for (const name of ['@humanlayer/channels-delivery', '@humanlayer/channels-slack']) {
	assertBuiltEntry(name, 'index')
	assertBuiltEntry(`${name}/memory`, 'memory')
}

const deliveryModule = await import(new URL('./MailboxStore.js', import.meta.resolve('@humanlayer/channels-delivery')))
const connectionModule = await import(
	new URL('./SlackConnectionStore.js', import.meta.resolve('@humanlayer/channels-slack'))
)
const subscriptionModule = await import(
	new URL('./SlackSubscriptions.js', import.meta.resolve('@humanlayer/channels-slack'))
)
assert.equal(Delivery.MailboxStore, deliveryModule.MailboxStore)
assert.equal(Delivery.MailboxReadiness, deliveryModule.MailboxReadiness)
assert.equal(Slack.SlackConnectionStore, connectionModule.SlackConnectionStore)
assert.equal(Slack.SlackSubscriptions, subscriptionModule.SlackSubscriptions)

const delivery = Effect.runSync(deliveryIdentity)
assert.equal(delivery.committed, 'committed')
assert.equal(delivery.snapshot?.revision, 0)
assert.deepEqual(delivery.ready, [])

const slack = Effect.runSync(slackIdentity)
assert.equal(slack.committed, 'committed')
assert.equal(slack.snapshot?.revision, 0)
assert.deepEqual(slack.ready, [])
assert.equal(slack.connection, undefined)
assert.equal(slack.subscribed, true)
assertOneEffect()
