import type { RunnerOptions } from '@humanlayer/channels-delivery'
import { Effect, Layer } from 'effect'

import { GitHub } from './GitHub.js'
import {
	GitHubIngress,
	type GitHubIngressOptions,
	type GitHubHandlerRegistration,
	type GitHubActivityRegistration,
} from './GitHubIngress.js'
import { GitHubRoutes } from './GitHubRoutes.js'

const assemble = <E, R>(options: GitHubIngressOptions<E, R> & { readonly runner: RunnerOptions }) => {
	const services = GitHubIngress.layer(options).pipe(Layer.provideMerge(GitHub.layer))
	const routes = GitHubRoutes.layerConfig.pipe(Layer.provide(services))
	const worker = Layer.effectDiscard(
		Effect.flatMap(GitHubIngress, (ingress) => ingress.run(options.runner)).pipe(Effect.forkScoped),
	).pipe(Layer.provide(services))
	return { services, routes, worker, layer: Layer.merge(routes, worker) }
}

type CallbackResult<T> = T extends (...args: never[]) => infer A ? A : never
type ActivityResult<T> = T extends GitHubActivityRegistration<unknown, unknown> ? CallbackResult<T[keyof T]> : never

function make<const H extends ReadonlyArray<GitHubActivityRegistration<unknown, unknown>>>(
	options: Omit<GitHubIngressOptions<unknown, unknown>, 'handlers' | 'activityHandlers'> & {
		readonly activityHandlers: H
		readonly runner: RunnerOptions
	},
): ReturnType<typeof assemble<Effect.Error<ActivityResult<H[number]>>, Effect.Services<ActivityResult<H[number]>>>>
function make<const H extends ReadonlyArray<GitHubHandlerRegistration<unknown, unknown>>>(
	options: Omit<GitHubIngressOptions<unknown, unknown>, 'handlers'> & {
		readonly handlers: H
		readonly runner: RunnerOptions
	},
): ReturnType<
	typeof assemble<Effect.Error<ReturnType<H[number]['handler']>>, Effect.Services<ReturnType<H[number]['handler']>>>
>
function make(
	options: Omit<GitHubIngressOptions<unknown, unknown>, 'handlers'> & {
		readonly handlers?: ReadonlyArray<GitHubHandlerRegistration<unknown, unknown>>
		readonly runner: RunnerOptions
	},
) {
	return assemble({ ...options, handlers: options.handlers ?? [] })
}

export const GitHubBot = { make }
