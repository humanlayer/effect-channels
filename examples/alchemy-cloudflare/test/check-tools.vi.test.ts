import { describe, it } from '@effect/vitest'
import {
	GitHubActionsJob,
	GitHubApi,
	GitHubCheckRun,
	GitHubCheckRunInfo,
	GitHubEventId,
	GitHubId,
	GitHubParticipant,
	GitHubPrCheckCompleted,
	GitHubPrCommentCreated,
	GitHubPullRequest,
	GitHubPullRequestInfo,
	GitHubPullRequestRef,
	GitHubSubscribedPrEvents,
	GitHubIssueComment,
	type GitHubCheckConclusion,
} from '@humanlayer/channels-github'
import { OutputStore, OutputStoreRef } from '@humanlayer/fold-agent'
import { CurrentToolCall, ToolCallId } from '@humanlayer/fold-core'
import { RuntimeContext } from 'alchemy/RuntimeContext'
import { Effect, Layer, Ref } from 'effect'

import { makeTestDeliveryExecution } from '../../../packages/delivery/test/delivery-execution'
import { AgentSessions } from '../src/AgentSessionDO'
import { AutoLabel } from '../src/AutoLabel'
import { describeCheckFailure, listPullRequestChecks } from '../src/CheckTools'
import type { AgentSessionMessage } from '../src/DeliveryTurn'
import { checkFailurePrompt, failedChecksOnHead, githubHandlers } from '../src/GithubBot'

const repository = {
	installationId: GitHubId.make(100),
	repositoryId: GitHubId.make(200),
	owner: 'humanlayer',
	repository: 'effect-channels',
}
const pullRequest = GitHubPullRequest.make({
	ref: GitHubPullRequestRef.make({ ...repository, number: GitHubId.make(43) }),
	mailboxKey: 'github:pr:43',
})
const HEAD = 'aaaaaaa1111'
const alice = GitHubParticipant.make({ id: GitHubId.make(1), login: 'alice', type: 'User' })

const checkInfo = (id: number, name: string, headSha: string, conclusion: GitHubCheckConclusion | null) =>
	GitHubCheckRunInfo.make({
		ref: { ...repository, id: GitHubId.make(id) },
		name,
		headSha,
		status: 'completed',
		conclusion,
		startedAt: null,
		completedAt: null,
		apiUrl: `https://api.github.com/repos/humanlayer/effect-channels/check-runs/${id}`,
		url: null,
		detailsUrl: `https://github.com/humanlayer/effect-channels/actions/runs/1/job/${id}`,
		checkSuiteId: null,
		outputTitle: conclusion === 'failure' ? '2 tests failed' : null,
		outputSummary: null,
		outputText: null,
		annotationCount: conclusion === 'failure' ? 1 : 0,
	})

const infos = [
	checkInfo(1, 'build', HEAD, 'success'),
	checkInfo(2, 'test', HEAD, 'failure'),
	checkInfo(3, 'old', 'bbb', 'failure'),
]

const apiWithLog = (log: string) =>
	Layer.mock(GitHubApi, {
		fetchPullRequest: () =>
			Effect.succeed(
				GitHubPullRequestInfo.make({
					ref: pullRequest.ref,
					title: 'Fix the crash',
					body: null,
					state: 'open',
					url: 'https://github.com/humanlayer/effect-channels/pull/43',
					author: alice,
					draft: false,
					merged: false,
					headRef: 'fix',
					headSha: HEAD,
					headRepository: null,
					baseRef: 'main',
					baseSha: 'def',
				}),
			),
		listCheckRunsForRef: () => Effect.succeed(infos.map((info) => GitHubCheckRun.make({ ref: info.ref }))),
		fetchCheckRun: ({ checkRun }) => Effect.succeed(infos.find((info) => info.ref.id === checkRun.id) ?? infos[0]!),
		listCheckRunAnnotations: () =>
			Effect.succeed([
				{
					path: 'src/app.test.ts',
					startLine: 12,
					endLine: 12,
					startColumn: null,
					endColumn: null,
					level: 'failure' as const,
					title: 'app handles null config',
					message: 'expected null to be "default"',
					rawDetails: null,
					blobUrl: 'https://github.com/humanlayer/effect-channels/blob/aaa/src/app.test.ts',
				},
			]),
		resolveActionsJob: () =>
			Effect.succeed(GitHubActionsJob.make({ ref: { ...repository, id: GitHubId.make(900) } })),
		fetchActionsJob: () =>
			Effect.succeed({
				ref: { ...repository, id: GitHubId.make(900) },
				runId: GitHubId.make(1),
				name: 'test',
				status: 'completed' as const,
				conclusion: 'failure' as const,
				headSha: HEAD,
				apiUrl: 'https://api.github.com/x',
				url: null,
				startedAt: null,
				completedAt: null,
				checkRunUrl: 'https://api.github.com/y',
				steps: [
					{
						name: 'Install',
						status: 'completed' as const,
						conclusion: 'success' as const,
						number: 1,
						startedAt: null,
						completedAt: null,
					},
					{
						name: 'Run tests',
						status: 'completed' as const,
						conclusion: 'failure' as const,
						number: 2,
						startedAt: null,
						completedAt: null,
					},
				],
			}),
		downloadActionsJobLog: () => Effect.succeed(log),
	})

const api = apiWithLog('FAIL src/app.test.ts\n  expected null to be "default"')

const toolCall = Layer.mergeAll(
	Layer.mock(CurrentToolCall, { toolCallId: ToolCallId.make('tool_call_a00000030000000000000000') }),
	Layer.mock(OutputStore, {
		directory: '/workspace/.fold/tool-output',
		refFor: (toolCallId) =>
			OutputStoreRef.make({ toolCallId, path: `/workspace/.fold/tool-output/${toolCallId}.log` }),
		append: (toolCallId) =>
			Effect.succeed(OutputStoreRef.make({ toolCallId, path: `/workspace/.fold/tool-output/${toolCallId}.log` })),
	}),
)

describe('github_pull_request_checks', () => {
	it.effect("lists the latest commit's checks, with IDs, and leaves out older commits'", ({ expect }) =>
		Effect.gen(function* () {
			const result = yield* listPullRequestChecks(pullRequest).pipe(Effect.provide(api))
			const check = (info: GitHubCheckRunInfo) => ({
				id: info.ref.id,
				name: info.name,
				status: info.status,
				conclusion: info.conclusion,
				title: info.outputTitle,
				summary: info.outputSummary,
				annotations: info.annotationCount,
				url: info.detailsUrl,
			})

			expect(result.text).toBe(
				JSON.stringify({ headSha: HEAD, checks: [check(infos[0]!), check(infos[1]!)] }, null, 2),
			)
		}),
	)
})

describe('github_check_failure', () => {
	it.effect('shows the failure, its annotations, the failed steps, and the job log', ({ expect }) =>
		Effect.gen(function* () {
			const result = yield* describeCheckFailure(pullRequest, 2).pipe(Effect.provide(Layer.merge(api, toolCall)))

			expect(result.text).toBe(
				[
					'Check "test" (check run 2) on aaaaaaa: failure. https://github.com/humanlayer/effect-channels/actions/runs/1/job/2',
					'Output:\n2 tests failed',
					'Annotations:\n- failure at src/app.test.ts:12: app handles null config: expected null to be "default"',
					'Failed steps in job "test":\n- 2. Run tests: failure',
					'Job log:\nFAIL src/app.test.ts\n  expected null to be "default"',
				].join('\n\n'),
			)
		}),
	)
})

describe('github_check_failure with a long log', () => {
	it.effect('shows the end of the log and saves all of it in the workspace', ({ expect }) =>
		Effect.gen(function* () {
			const log = Array.from({ length: 3_000 }, (_, line) => `line ${line + 1}`).join('\n')
			const result = yield* describeCheckFailure(pullRequest, 2).pipe(
				Effect.provide(Layer.merge(apiWithLog(log), toolCall)),
			)

			expect(result.text).toContain('line 3000')
			expect(result.text).not.toContain('line 1\n')
			expect(result.text).toContain(
				'Full output: /workspace/.fold/tool-output/tool_call_a00000030000000000000000.log',
			)
		}),
	)
})

describe('failed checks on a followed pull request', () => {
	const completed = (id: number, name: string, conclusion: GitHubCheckConclusion, headSha = HEAD) =>
		GitHubPrCheckCompleted.make({
			pullRequest,
			actor: alice,
			eventId: GitHubEventId.make(`check-${id}-${headSha}`),
			checkRunId: GitHubId.make(id),
			name,
			status: 'completed',
			conclusion,
			detailsUrl: `https://github.com/humanlayer/effect-channels/actions/runs/1/job/${id}`,
			headSha,
			checkSuiteId: null,
			startedAt: null,
			completedAt: null,
		})

	it('keeps failures on the latest commit, each once, and ignores the rest', ({ expect }) => {
		const comment = GitHubPrCommentCreated.make({
			pullRequest,
			actor: alice,
			eventId: GitHubEventId.make('comment-1'),
			comment: GitHubIssueComment.make({
				ref: { discussion: { _tag: 'PullRequest', ref: pullRequest.ref }, id: GitHubId.make(5) },
				body: 'looks good',
				url: 'https://github.com/humanlayer/effect-channels/pull/43#issuecomment-5',
				author: alice,
			}),
		})
		const events = [
			completed(1, 'build', 'success'),
			completed(2, 'test', 'failure'),
			completed(2, 'test', 'failure'),
			completed(3, 'lint', 'timed_out'),
			completed(4, 'deploy', 'skipped'),
			completed(5, 'test', 'failure', 'bbbbbbb'),
			comment,
		]

		expect(failedChecksOnHead(events, HEAD).map(({ checkRunId }) => checkRunId)).toEqual([2, 3])
	})

	it('asks the agent to find the cause and fix it if it can', ({ expect }) => {
		expect(checkFailurePrompt([completed(2, 'test', 'failure'), completed(3, 'lint', 'timed_out')], HEAD)).toBe(
			[
				"<system-information>Checks failed on this pull request's latest commit, aaaaaaa. Nobody mentioned you; these failures are the request.</system-information>",
				'',
				'- `test`: failure (check run 2), https://github.com/humanlayer/effect-channels/actions/runs/1/job/2',
				'- `lint`: timed out (check run 3), https://github.com/humanlayer/effect-channels/actions/runs/1/job/3',
				'',
				'Find out why with github_check_failure. If the cause is clear and the fix belongs in this pull request, fix it, check it as far as you can, and push. Otherwise explain the cause and what to change. Your answer is posted on the pull request.',
			].join('\n'),
		)
	})

	it.effect('start a turn on the latest commit, and hand the delivery off', ({ expect }) =>
		Effect.gen(function* () {
			const delivery = yield* makeTestDeliveryExecution(pullRequest.mailboxKey)
			const sent = yield* Ref.make<ReadonlyArray<typeof AgentSessionMessage.Encoded>>([])
			const agentSessions = AgentSessions.of({
				getByName: () => ({ send: (message) => Ref.update(sent, (all) => [...all, message]) }),
			})
			const services = Layer.mergeAll(
				api,
				Layer.succeed(AgentSessions, agentSessions),
				Layer.mock(AutoLabel, {}),
				Layer.mock(RuntimeContext, { Type: 'Worker', id: 'check-failure-test', env: {} }),
			)
			const run = (events: readonly [GitHubPrCheckCompleted, ...Array<GitHubPrCheckCompleted>]) =>
				githubHandlers
					.onSubscribedPrEvents(
						GitHubSubscribedPrEvents.make({ pullRequest, events }),
						delivery.execution.context,
					)
					.pipe(Effect.provide(services))

			yield* run([completed(4, 'deploy', 'success'), completed(5, 'test', 'failure', 'bbbbbbb')])
			expect(yield* Ref.get(sent)).toEqual([])
			expect(yield* Ref.get(delivery.handoffs)).toEqual([])

			yield* run([completed(2, 'test', 'failure')])
			const [message] = yield* Ref.get(sent)
			expect(message?.prompt).toBe(checkFailurePrompt([completed(2, 'test', 'failure')], HEAD))
			expect(message?.requestComments).toEqual([])
			expect(yield* Ref.get(delivery.handoffs)).toHaveLength(1)
		}),
	)
})
