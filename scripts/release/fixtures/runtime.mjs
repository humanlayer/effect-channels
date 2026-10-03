// Imports every published package under plain Node.js, with the guard loaded first.
import assert from 'node:assert/strict'

import { assertOneEffect } from './guard.mjs'

const packages = {
	'@humanlayer/channels-delivery': ['Channels', 'ChannelsMemory', 'makeDeliveryClient', 'DeliveryHttpApi'],
	'@humanlayer/channels-slack': ['SlackBot', 'SlackContent'],
	'@humanlayer/channels-github': ['GitHubBot', 'GitHubContent'],
	'@humanlayer/channels-linear': ['LinearBot', 'LinearAuth'],
	'@humanlayer/channels-sql': ['ChannelsSql'],
	'@humanlayer/channels-redis': ['ChannelsRedis'],
	'@humanlayer/channels-alchemy-cloudflare': ['ChannelsCloudflare'],
}

for (const [name, exports] of Object.entries(packages)) {
	const module = await import(name)
	for (const exported of exports) assert.ok(module[exported] !== undefined, `${name} does not export ${exported}`)
	assert.ok(import.meta.resolve(name).includes(`/node_modules/${name}/dist/`), `${name} did not resolve to dist/`)
	console.log(`imported ${name}`)
}
assertOneEffect()
