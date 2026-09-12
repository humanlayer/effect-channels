import assert from 'node:assert/strict'
import { createHook } from 'node:async_hooks'
import { registerHooks } from 'node:module'

const consumer = new URL('./', import.meta.url).href
const effectRoots = new Set()
export const resolvedModules = new Set()
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
		assert.ok(!networkResources.has(type), 'Package import attempted to acquire a network resource')
	},
}).enable()

registerHooks({
	resolve(specifier, context, nextResolve) {
		const resolved = nextResolve(specifier, context)
		if (resolved.url.startsWith('file:')) {
			resolvedModules.add(resolved.url)
			assert.ok(resolved.url.startsWith(consumer), 'Module resolution escaped the isolated consumer')
			assert.ok(!resolved.url.endsWith('.ts'), 'Node loaded TypeScript instead of built JavaScript')
			const marker = '/node_modules/effect/'
			const index = resolved.url.lastIndexOf(marker)
			if (index !== -1) effectRoots.add(resolved.url.slice(0, index + marker.length))
		}
		return resolved
	},
})

export const assertOneEffect = () => assert.equal(effectRoots.size, 1, 'Expected exactly one resolved Effect runtime')

export const assertBuiltEntry = (specifier, entry) => {
	const resolved = import.meta.resolve(specifier)
	assert.ok(resolved.startsWith(consumer), 'Package resolved outside consumer')
	assert.ok(resolved.endsWith(`/dist/${entry}.js`), 'Package did not resolve its built ESM entry')
}
