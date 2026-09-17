import { Effect, Layer } from 'effect'

import { GitHub } from './GitHub'
import { GitHubIngress, type GitHubIngressOptions, type GitHubHandlerRegistration } from './GitHubIngress'

const assemble = <E, R>(options: GitHubIngressOptions<E, R>) => {
	const services = GitHubIngress.layer(options).pipe(Layer.provideMerge(GitHub.layer))
	return services
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
	},
): ReturnType<typeof assemble<Effect.Error<HandlerResult<H[number]>>, Effect.Services<HandlerResult<H[number]>>>>
function make(options: GitHubIngressOptions<unknown, unknown>) {
	return assemble(options)
}

export const GitHubBot = { make }
