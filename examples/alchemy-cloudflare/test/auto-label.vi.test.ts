import { describe, it } from '@effect/vitest'
import {
	GitHubApi,
	GitHubApiError,
	GitHubCallbacks,
	GitHubEventId,
	GitHubId,
	GitHubIssue,
	GitHubIssueCreated,
	GitHubIssueOpened,
	GitHubIssueRef,
	GitHubLabel,
	GitHubParticipant,
	GitHubPrCreated,
	GitHubPrOpened,
	GitHubPullRequest,
	GitHubPullRequestRef,
	type GitHubRepositoryRequest,
} from '@humanlayer/channels-github'
import * as Cloudflare from 'alchemy/Cloudflare'
import { RuntimeContext } from 'alchemy/RuntimeContext'
import { ConfigProvider, Deferred, Effect, Fiber, Layer, Logger, Ref } from 'effect'
import { TestClock } from 'effect/testing'

import { makeTestDeliveryExecution } from '../../../packages/delivery/test/delivery-execution'
import { AutoLabel } from '../src/AutoLabel'
import { githubHandlers } from '../src/GithubBot'

const repository = {
	installationId: GitHubId.make(100),
	repositoryId: GitHubId.make(200),
	owner: 'humanlayer',
	repository: 'effect-channels',
}
const issue = GitHubIssue.make({
	ref: GitHubIssueRef.make({ ...repository, number: GitHubId.make(42) }),
	mailboxKey: 'github:issue:42',
})
const pullRequest = GitHubPullRequest.make({
	ref: GitHubPullRequestRef.make({ ...repository, number: GitHubId.make(43) }),
	mailboxKey: 'github:pr:43',
})
const label = (name: string) => GitHubLabel.make({ name, color: 'ffffff', description: null })
const answer = (noul: number) => ({ type: 'noul', noul })
const input = { discussion: issue, kind: 'issue', title: 'Broken feature', body: 'Private report body' } as const
const apply = (value: Parameters<AutoLabel['Service']['apply']>[0] = input) =>
	Effect.flatMap(AutoLabel, (service) => service.apply(value))

type NativeBinding = Effect.Success<Cloudflare.Workers.AIClient['raw']>
type NativeResponse = Awaited<ReturnType<NativeBinding['run']>>
type NativeInputs = Parameters<NativeBinding['run']>[1]

const makeHarness = (
	options: {
		readonly labels?: ReadonlyArray<string>
		readonly response?: NativeResponse
		readonly run?: () => Promise<NativeResponse>
		readonly config?: {
			readonly GITHUB_LABEL_MODEL?: string
			readonly GITHUB_LABEL_THRESHOLD?: number
			readonly GITHUB_LABEL_TIMEOUT?: string
		}
		readonly lookup?: Effect.Effect<void, GitHubApiError>
		readonly writeError?: GitHubApiError
		readonly missingBinding?: boolean
	} = {},
) =>
	Effect.gen(function* () {
		const requests: Array<{ model: string; payload: NativeInputs }> = []
		const logs: Array<string> = []
		const lookups = yield* Ref.make<ReadonlyArray<GitHubRepositoryRequest>>([])
		const writes = yield* Ref.make<ReadonlyArray<{ kind: string; number: number; labels: ReadonlyArray<string> }>>(
			[],
		)
		const existing = yield* Ref.make<ReadonlyArray<string>>(['custom-existing'])
		const add = (kind: string, number: number, labels: ReadonlyArray<string>) =>
			Effect.gen(function* () {
				yield* Ref.update(writes, (previous) => [...previous, { kind, number, labels }])
				if (options.writeError !== undefined) return yield* options.writeError
				const names = yield* Ref.updateAndGet(existing, (previous) => [...new Set([...previous, ...labels])])
				return names.map(label)
			})
		const dependencies = Layer.mergeAll(
			Layer.mock(RuntimeContext, { Type: 'Worker', id: 'auto-label-test', env: {} }),
			Layer.mock(GitHubApi, {
				listRepositoryLabels: (request) =>
					Ref.update(lookups, (previous) => [...previous, request]).pipe(
						Effect.andThen(options.lookup ?? Effect.void),
						Effect.as((options.labels ?? ['bug']).map(label)),
					),
				addIssueLabels: ({ issue, labels }) => add('issue', issue.number, labels),
				addPullRequestLabels: ({ pullRequest, labels }) => add('pull_request', pullRequest.number, labels),
			}),
			ConfigProvider.layer(ConfigProvider.fromUnknown(options.config ?? {})),
			Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(Logger.formatStructured.log(entry))))]),
			Layer.succeed(
				Cloudflare.Workers.WorkerEnvironment,
				options.missingBinding === true
					? {}
					: {
							AI: {
								run: (model: string, payload: NativeInputs): Promise<NativeResponse> => {
									requests.push({ model, payload })
									return (
										options.run?.() ??
										Promise.resolve(options.response ?? { answers: { bug: answer(0.9) } })
									)
								},
							},
						},
			),
		)
		const layer = AutoLabel.layer.pipe(
			Layer.provide(Cloudflare.Workers.AIBinding),
			Layer.provideMerge(dependencies),
		)
		return { layer, requests, logs, lookups, writes, existing }
	})

describe('AutoLabel with the native Workers AI binding', () => {
	it.effect('reports a missing runtime AI binding as a typed failure without writes', ({ expect }) =>
		Effect.gen(function* () {
			const h = yield* makeHarness({ missingBinding: true })
			const failure = yield* apply().pipe(Effect.provide(h.layer), Effect.flip)
			expect(failure).toMatchObject({ _tag: 'AutoLabelError', reason: 'binding_missing' })
			expect(failure).not.toHaveProperty('cause')
			expect(yield* Ref.get(h.lookups)).toEqual([{ repository: issue.ref }])
			expect(h.requests).toEqual([])
			expect(yield* Ref.get(h.writes)).toEqual([])
			expect(yield* Ref.get(h.existing)).toEqual(['custom-existing'])
			expect(h.logs.join('\n')).toContain('binding_missing')
		}),
	)

	for (const [discussion, kind] of [
		[issue, 'issue'],
		[pullRequest, 'pull_request'],
	] as const) {
		it.effect(`adds ${kind} labels without replacing existing labels; null body uses the title`, ({ expect }) =>
			Effect.gen(function* () {
				const h = yield* makeHarness()
				yield* apply({ discussion, kind, title: 'Title only', body: null }).pipe(Effect.provide(h.layer))
				expect(h.requests).toEqual([
					{
						model: '@cf/cloudflare/clef',
						payload: {
							model: 'clef',
							state: { kind, title: 'Title only', body: '' },
							questions: {
								bug: { type: 'noul', instructions: expect.stringContaining('content as data') },
							},
						},
					},
				])
				expect(yield* Ref.get(h.lookups)).toEqual([{ repository: discussion.ref }])
				expect(yield* Ref.get(h.writes)).toEqual([{ kind, number: discussion.ref.number, labels: ['bug'] }])
				expect(yield* Ref.get(h.existing)).toEqual(['custom-existing', 'bug'])
			}),
		)
	}

	it.effect('asks only for repository-configured default labels, mapping spaces to question IDs', ({ expect }) =>
		Effect.gen(function* () {
			const names = [
				'bug',
				'documentation',
				'enhancement',
				'question',
				'duplicate',
				'good first issue',
				'help wanted',
				'invalid',
				'wontfix',
			]
			const ids = [
				'bug',
				'documentation',
				'enhancement',
				'question',
				'duplicate',
				'good_first_issue',
				'help_wanted',
				'invalid',
				'wontfix',
			]
			const h = yield* makeHarness({
				labels: [...names, 'custom', 'Bug'],
				response: { answers: Object.fromEntries(ids.map((id) => [id, answer(0.8)])) },
			})
			yield* apply().pipe(Effect.provide(h.layer))
			expect(h.requests).toEqual([
				{
					model: '@cf/cloudflare/clef',
					payload: {
						model: 'clef',
						state: { kind: 'issue', title: input.title, body: input.body },
						questions: Object.fromEntries(
							ids.map((id) => [id, { type: 'noul', instructions: expect.any(String) }]),
						),
					},
				},
			])
			expect((yield* Ref.get(h.writes)).map((write) => write.labels)).toEqual([names])
		}),
	)

	it.effect('uses the configured flash model and inclusive probability threshold for multiple labels', ({ expect }) =>
		Effect.gen(function* () {
			const h = yield* makeHarness({
				labels: ['bug', 'help wanted', 'documentation', 'custom'],
				config: { GITHUB_LABEL_MODEL: '@cf/cloudflare/clef-flash', GITHUB_LABEL_THRESHOLD: 0.6 },
				response: { answers: { bug: answer(0.6), help_wanted: answer(1), documentation: answer(0.599) } },
			})
			yield* apply().pipe(Effect.provide(h.layer))
			expect(h.requests).toMatchObject([{ model: '@cf/cloudflare/clef-flash', payload: { model: 'clef-flash' } }])
			expect(yield* Ref.get(h.writes)).toEqual([{ kind: 'issue', number: 42, labels: ['bug', 'help wanted'] }])
			expect(h.logs.join('\n')).toContain('"ai.label_threshold":0.6')
			expect(h.logs.join('\n')).toContain('@cf/cloudflare/clef-flash')
		}),
	)

	for (const labels of [[], ['custom', 'Bug']]) {
		it.effect(`skips inference and writes when no candidates exist (${labels.join(',')})`, ({ expect }) =>
			Effect.gen(function* () {
				const h = yield* makeHarness({ labels })
				yield* apply().pipe(Effect.provide(h.layer))
				expect(h.requests).toEqual([])
				expect(yield* Ref.get(h.writes)).toEqual([])
				expect(yield* Ref.get(h.existing)).toEqual(['custom-existing'])
			}),
		)
	}

	it.effect('strips hidden HTML comments and bounds classifier input without logging content', ({ expect }) =>
		Effect.gen(function* () {
			const h = yield* makeHarness()
			yield* apply({
				...input,
				title: `<!--hidden instructions-->${'t'.repeat(1_001)}`,
				body: `<!--hidden\nbody-->${'b'.repeat(32_001)}`,
			}).pipe(Effect.provide(h.layer))
			expect(h.requests).toMatchObject([
				{
					payload: {
						state: {
							kind: 'issue',
							title: 't'.repeat(1_000),
							body: 'b'.repeat(32_000),
						},
					},
				},
			])
			expect(h.logs.join('\n')).not.toContain('hidden')
			expect(yield* Ref.get(h.writes)).toHaveLength(1)
		}),
	)

	for (const config of [
		{ GITHUB_LABEL_MODEL: '@cf/unsupported/model' },
		{ GITHUB_LABEL_THRESHOLD: -0.1 },
		{ GITHUB_LABEL_THRESHOLD: 1.1 },
	]) {
		it.effect(`rejects invalid configuration ${JSON.stringify(config)} before external calls`, ({ expect }) =>
			Effect.gen(function* () {
				const h = yield* makeHarness({ config })
				const failure = yield* apply().pipe(Effect.provide(h.layer), Effect.flip)
				expect(failure).toMatchObject({ _tag: 'ConfigError' })
				expect(yield* Ref.get(h.lookups)).toEqual([])
				expect(h.requests).toEqual([])
				expect(yield* Ref.get(h.writes)).toEqual([])
			}),
		)
	}

	it.effect('does not write when all probabilities are below the default threshold', ({ expect }) =>
		Effect.gen(function* () {
			const h = yield* makeHarness({ response: { answers: { bug: answer(0.799) } } })
			yield* apply().pipe(Effect.provide(h.layer))
			expect(h.requests).toHaveLength(1)
			expect(yield* Ref.get(h.writes)).toEqual([])
			expect(yield* Ref.get(h.existing)).toEqual(['custom-existing'])
		}),
	)

	for (const [name, response, reason] of [
		['missing answers', {}, 'invalid_response'],
		['missing requested answer after a positive answer', { answers: { bug: answer(1) } }, 'missing_answer'],
		['probability above one', { answers: { bug: answer(1), documentation: answer(1.01) } }, 'invalid_response'],
		['negative probability', { answers: { bug: answer(1), documentation: answer(-0.1) } }, 'invalid_response'],
		[
			'string probability',
			{ answers: { bug: answer(1), documentation: { type: 'noul', noul: '0.9' } } },
			'invalid_response',
		],
		[
			'wrong answer type',
			{ answers: { bug: answer(1), documentation: { type: 'bool', noul: 0.9 } } },
			'invalid_response',
		],
	] as const) {
		it.effect(`rejects ${name} before any label writes`, ({ expect }) =>
			Effect.gen(function* () {
				const h = yield* makeHarness({ labels: ['bug', 'documentation'], response })
				const failure = yield* apply().pipe(Effect.provide(h.layer), Effect.flip)
				expect(failure).toMatchObject({ _tag: 'AutoLabelError', reason })
				expect(h.requests).toHaveLength(1)
				expect(yield* Ref.get(h.writes)).toEqual([])
				expect(yield* Ref.get(h.existing)).toEqual(['custom-existing'])
			}),
		)
	}

	it.effect('maps native rejection and logs safe metadata, not bodies, titles or credentials', ({ expect }) =>
		Effect.gen(function* () {
			const secret = 'credential-do-not-log'
			const h = yield* makeHarness({ run: () => Promise.reject(new Error(`${secret}: ${input.body}`)) })
			const failure = yield* apply().pipe(Effect.provide(h.layer), Effect.flip)
			expect(failure).toMatchObject({ _tag: 'AutoLabelError', reason: 'inference_failed' })
			expect(h.requests).toHaveLength(1)
			expect(yield* Ref.get(h.writes)).toEqual([])
			const logs = h.logs.join('\n')
			expect(logs).toContain('GitHub auto-label failed')
			expect(logs).toContain('inference_failed')
			expect(logs).toContain('"github.number":42')
			expect(logs).toContain('"ai.label_threshold":0.8')
			for (const sensitive of [secret, input.title, input.body]) expect(logs).not.toContain(sensitive)
		}),
	)

	for (const [config, duration] of [
		[{}, '60 seconds'],
		[{ GITHUB_LABEL_TIMEOUT: '2 seconds' }, '2 seconds'],
	] as const) {
		it.effect(`times out native inference at ${duration} without writes`, ({ expect }) =>
			Effect.gen(function* () {
				const h = yield* makeHarness({ config, run: () => Promise.withResolvers<NativeResponse>().promise })
				const fiber = yield* apply().pipe(Effect.provide(h.layer), Effect.flip, Effect.forkChild)
				yield* TestClock.adjust(duration)
				expect(yield* Fiber.join(fiber)).toMatchObject({ _tag: 'AutoLabelError', reason: 'timed_out' })
				expect(h.requests).toHaveLength(1)
				expect(yield* Ref.get(h.writes)).toEqual([])
			}),
		)
	}

	for (const stage of ['lookup', 'write'] as const) {
		it.effect(`propagates GitHub ${stage} failures`, ({ expect }) =>
			Effect.gen(function* () {
				const error = GitHubApiError.make({
					operation: stage === 'lookup' ? 'list_repository_labels' : 'add_issue_labels',
					reason: 'unavailable',
					retryable: true,
				})
				const h = yield* makeHarness(
					stage === 'lookup' ? { lookup: Effect.fail(error) } : { writeError: error },
				)
				expect(yield* apply().pipe(Effect.provide(h.layer), Effect.flip)).toEqual(error)
				expect(h.requests).toHaveLength(stage === 'lookup' ? 0 : 1)
				expect(yield* Ref.get(h.writes)).toHaveLength(stage === 'lookup' ? 0 : 1)
				expect(yield* Ref.get(h.existing)).toEqual(['custom-existing'])
			}),
		)
	}

	it.effect('keeps overlapping host layers isolated for native env and configuration', ({ expect }) =>
		Effect.gen(function* () {
			const firstReady = yield* Deferred.make<void>()
			const secondReady = yield* Deferred.make<void>()
			const first = yield* makeHarness({
				lookup: Deferred.succeed(firstReady, undefined).pipe(Effect.andThen(Deferred.await(secondReady))),
			})
			const second = yield* makeHarness({
				lookup: Deferred.succeed(secondReady, undefined).pipe(Effect.andThen(Deferred.await(firstReady))),
				config: { GITHUB_LABEL_MODEL: '@cf/cloudflare/clef-flash', GITHUB_LABEL_THRESHOLD: 0.95 },
			})
			yield* Effect.all(
				[
					apply().pipe(Effect.provide(first.layer)),
					apply({ discussion: pullRequest, kind: 'pull_request', title: 'Second host', body: null }).pipe(
						Effect.provide(second.layer),
					),
				],
				{ concurrency: 'unbounded' },
			)
			expect(first.requests).toMatchObject([
				{ model: '@cf/cloudflare/clef', payload: { state: { title: input.title } } },
			])
			expect(second.requests).toMatchObject([
				{ model: '@cf/cloudflare/clef-flash', payload: { state: { title: 'Second host' } } },
			])
			expect(yield* Ref.get(first.writes)).toHaveLength(1)
			expect(yield* Ref.get(second.writes)).toEqual([])
		}),
	)
})

describe('registered GitHub creation handlers', () => {
	for (const kind of ['issue', 'pull_request'] as const) {
		it.effect(`${kind} creation invokes the real AutoLabel through GitHubCallbacks`, ({ expect }) =>
			Effect.gen(function* () {
				const h = yield* makeHarness()
				const actor = GitHubParticipant.make({ id: GitHubId.make(1), login: 'author', type: 'User' })
				const eventId = GitHubEventId.make('created')
				const { execution } = yield* makeTestDeliveryExecution()
				yield* Effect.gen(function* () {
					const callbacks = yield* GitHubCallbacks
					if (kind === 'issue') {
						if (callbacks.onIssueCreated === undefined)
							return yield* Effect.die('onIssueCreated is not registered')
						const trigger = GitHubIssueOpened.make({
							issue,
							actor,
							eventId,
							title: 'New issue',
							body: null,
						})
						return yield* callbacks.onIssueCreated(
							GitHubIssueCreated.make({ issue, trigger, events: [] }),
							execution.context,
						)
					} else {
						if (callbacks.onPrCreated === undefined)
							return yield* Effect.die('onPrCreated is not registered')
						const trigger = GitHubPrOpened.make({
							pullRequest,
							actor,
							eventId,
							title: 'New PR',
							body: 'PR body',
						})
						return yield* callbacks.onPrCreated(
							GitHubPrCreated.make({ pullRequest, trigger, events: [] }),
							execution.context,
						)
					}
				}).pipe(Effect.provide(GitHubCallbacks.layer(githubHandlers).pipe(Layer.provideMerge(h.layer))))
				expect(h.requests).toMatchObject([
					{
						payload: {
							state:
								kind === 'issue'
									? { kind, title: 'New issue', body: '' }
									: { kind, title: 'New PR', body: 'PR body' },
						},
					},
				])
				expect(yield* Ref.get(h.writes)).toEqual([
					{ kind, number: kind === 'issue' ? 42 : 43, labels: ['bug'] },
				])
				const successLog = h.logs.find((entry) => entry.includes('GitHub labels added'))
				expect(successLog).toContain(`"delivery.id":${JSON.stringify(execution.context.deliveryId)}`)
				expect(successLog).toContain(`"github.event_id":${JSON.stringify(eventId)}`)
			}),
		)

		it.effect(`${kind} creation maps native inference failure through GitHubCallbacks`, ({ expect }) =>
			Effect.gen(function* () {
				const title = 'Submission title'
				const body = 'Submission body'
				const nativeError = new Error('native inference failed')
				const h = yield* makeHarness({ run: () => Promise.reject(nativeError) })
				const actor = GitHubParticipant.make({ id: GitHubId.make(1), login: 'author', type: 'User' })
				const eventId = GitHubEventId.make('failed-creation')
				const { execution } = yield* makeTestDeliveryExecution()
				const failure = yield* Effect.gen(function* () {
					const callbacks = yield* GitHubCallbacks
					if (kind === 'issue') {
						if (callbacks.onIssueCreated === undefined)
							return yield* Effect.die('onIssueCreated is not registered')
						const trigger = GitHubIssueOpened.make({ issue, actor, eventId, title, body })
						return yield* callbacks.onIssueCreated(
							GitHubIssueCreated.make({ issue, trigger, events: [] }),
							execution.context,
						)
					}
					if (callbacks.onPrCreated === undefined) return yield* Effect.die('onPrCreated is not registered')
					const trigger = GitHubPrOpened.make({ pullRequest, actor, eventId, title, body })
					return yield* callbacks.onPrCreated(
						GitHubPrCreated.make({ pullRequest, trigger, events: [] }),
						execution.context,
					)
				}).pipe(
					Effect.provide(GitHubCallbacks.layer(githubHandlers).pipe(Layer.provideMerge(h.layer))),
					Effect.flip,
				)
				expect(failure).toMatchObject({
					_tag: 'GitHubCallbackError',
					callback: kind === 'issue' ? 'onIssueCreated' : 'onPrCreated',
					reason: 'failed',
				})
				expect(h.requests).toHaveLength(1)
				expect(yield* Ref.get(h.writes)).toEqual([])
				expect(yield* Ref.get(h.existing)).toEqual(['custom-existing'])
				const logs = h.logs.join('\n')
				expect(logs).toContain('GitHub auto-label failed')
				expect(logs).toContain('inference_failed')
				expect(logs).toContain('GitHub application callback failed')
			}),
		)
	}
})
