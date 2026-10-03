// Loaded before the runtime check. Fails if importing a package opens a network connection, loads
// TypeScript source, resolves outside the throwaway project, or brings in a second copy of Effect.
import assert from 'node:assert/strict'
import { createHook } from 'node:async_hooks'
import { registerHooks } from 'node:module'

const consumer = new URL('./', import.meta.url).href
const effectRoots = new Set()
const networkResources = new Set([
	'TCPWRAP',
	'TCPCONNECTWRAP',
	'TLSWRAP',
	'GETADDRINFOREQWRAP',
	'GETNAMEINFOREQWRAP',
	'UDPWRAP',
	'UDPSENDWRAP',
	'QUERYWRAP',
	'HTTPCLIENTREQUEST',
])

createHook({
	init(_id, type) {
		assert.ok(!networkResources.has(type), `Importing a package opened a network resource (${type})`)
	},
}).enable()

registerHooks({
	resolve(specifier, context, nextResolve) {
		const resolved = nextResolve(specifier, context)
		if (resolved.url.startsWith('file:')) {
			assert.ok(resolved.url.startsWith(consumer), `Module resolution escaped the consumer: ${resolved.url}`)
			assert.ok(!/\.[cm]?ts$/.test(resolved.url), `Node loaded TypeScript instead of JavaScript: ${resolved.url}`)
			const marker = '/node_modules/effect/'
			const index = resolved.url.lastIndexOf(marker)
			if (index !== -1) effectRoots.add(resolved.url.slice(0, index + marker.length))
		}
		return resolved
	},
})

export const assertOneEffect = () =>
	assert.equal(effectRoots.size, 1, `Expected one copy of Effect, found ${[...effectRoots].join(', ')}`)
