import type { RunnerOptions } from '@humanlayer/channels-delivery'
import { Effect, Layer } from 'effect'

import { GitHub } from './GitHub.js'
import { GitHubIngress, type GitHubIngressOptions, type GitHubHandlerRegistration } from './GitHubIngress.js'
import { GitHubRoutes } from './GitHubRoutes.js'

const assemble = <E, R>(options: GitHubIngressOptions<E, R> & { readonly runner?: Partial<RunnerOptions> }) => {
	const services = GitHubIngress.layer(options).pipe(Layer.provideMerge(GitHub.layer))
	const routes = GitHubRoutes.layerConfig.pipe(Layer.provide(services))
	const worker = Layer.effectDiscard(
		Effect.flatMap(GitHubIngress, (ingress) =>
			ingress.run({ scanLimit: 100, concurrency: 8, pollMs: 25, ...options.runner }),
		).pipe(Effect.forkScoped),
	).pipe(Layer.provide(services))
	return { services, routes, worker, layer: Layer.merge(routes, worker) }
}

type CallbackResult<T> = T extends (...args: never[]) => infer A ? A : never
type HandlerResult<T> = T extends GitHubHandlerRegistration<unknown, unknown> ? CallbackResult<T[keyof T]> : never

function make<const H extends ReadonlyArray<GitHubHandlerRegistration<unknown, unknown>>>(
	options: Omit<GitHubIngressOptions<unknown, unknown>, 'handlers'> & {
		readonly handlers: H & {
			readonly [K in keyof H]: Record<
				Exclude<keyof H[K], keyof GitHubHandlerRegistration<unknown, unknown>>,
				never
			>
		}
		readonly runner?: Partial<RunnerOptions>
	},
): ReturnType<typeof assemble<Effect.Error<HandlerResult<H[number]>>, Effect.Services<HandlerResult<H[number]>>>>
function make(options: GitHubIngressOptions<unknown, unknown> & { readonly runner?: Partial<RunnerOptions> }) {
	return assemble(options)
}

export const GitHubBot = { make }
