import { describe, it } from '@effect/vitest'
import {
	BatchId,
	DeliveryActivity,
	DeliveryOperationId,
	DeliveryOutcome,
	DeliveryOutputApplied,
	ExternalLink,
	MessageId,
	PreparedDeliveryInvocation,
	ProviderOutputAttempt,
	ProviderPresentOutcome,
	makeDeliveryId,
	type ProviderOutputOperation,
} from '@humanlayer/channels-delivery-next'
import { Effect, Layer, Match, Ref, Result, Schema } from 'effect'

import { commentOutputScenarios } from '../../delivery-next/test/comment-output-scenarios'

import {
	LinearAgentSessionDestination,
	LinearDeliveryDestination,
	LinearIssueDestination,
	linearSupportedOperations,
} from '../src/LinearDeliveryDestination'
import { LinearOutputReceipt, linearDefaultOutcomeText, makeLinearOutputProcessor } from '../src/LinearDeliveryOutput'
import { LinearApi, LinearApiError } from '../src/LinearApi'
import {
	LinearAgentActivityId,
	LinearAgentSessionId,
	LinearCommentId,
	LinearIssueId,
	LinearOrganizationId,
	LinearUserId,
} from '../src/LinearIdentity'
import {
	LinearAgentActivityReceipt,
	LinearCommentRef,
	LinearContent,
	LinearIssueRef,
	type LinearCreateAgentActivityRequest,
	type LinearCreateCommentRequest,
	type LinearDeleteCommentRequest,
	type LinearUpdateAgentSessionRequest,
	type LinearUpdateCommentRequest,
} from '../src'
import { LinearComment } from '../src/LinearResources'
import { linearAppUserId, linearOrganizationId } from './fixtures'

const idempotencyKey = '5b0c3a1e-8d2f-4c61-9a57-1f2e3d4c5b6a'
const sessionId = LinearAgentSessionId.make('71000000-0000-4000-8000-000000000001')
const issueId = LinearIssueId.make('b33fb278-fbe0-45e4-b4eb-94b0839f51b9')
const bot = { organizationId: linearOrganizationId, appUserId: linearAppUserId }

const sessionDestination = LinearAgentSessionDestination.make({
	organizationId: linearOrganizationId,
	appUserId: linearAppUserId,
	sessionId,
	issueId,
})
const issueDestination = LinearIssueDestination.make({ organizationId: linearOrganizationId, issueId })

const prepared = (destination: LinearDeliveryDestination, presentationVersion = 1) =>
	PreparedDeliveryInvocation.make({
		callback: 'onAgentSessionCreated',
		presentationVersion,
		destination: Schema.encodeSync(Schema.toCodecJson(LinearDeliveryDestination))(destination),
		supportedOperations: linearSupportedOperations(destination),
	})

const attempt = (operation: ProviderOutputOperation, destination: LinearDeliveryDestination = sessionDestination) =>
	ProviderOutputAttempt.make({
		deliveryId: makeDeliveryId({ mailboxKey: 'linear:v1:agent-session:x', batchId: BatchId.make('batch-1') }),
		operationId: DeliveryOperationId.make('outcome'),
		attempt: 1,
		hadAmbiguousAttempt: false,
		idempotencyKey,
		prepared: prepared(destination),
		operation,
	})

const outcome = (result: DeliveryOutcome, markdown?: string) =>
	ProviderPresentOutcome.make(
		markdown === undefined ? { outcome: result, clearActivity: true } : { outcome: result, markdown, clearActivity: true },
	)

const commentRef = LinearCommentRef.make({
	organizationId: linearOrganizationId,
	teamId: null,
	issueId,
	commentId: LinearCommentId.make('72000000-0000-4000-8000-00000000c0de'),
})
const commentReceipt = Schema.encodeSync(Schema.toCodecJson(LinearOutputReceipt))({
	_tag: 'LinearIssueComment',
	comment: commentRef,
})

type Calls = {
	readonly activities: ReadonlyArray<LinearCreateAgentActivityRequest>
	readonly sessionUpdates: ReadonlyArray<LinearUpdateAgentSessionRequest>
	readonly comments: ReadonlyArray<LinearCreateCommentRequest>
	readonly commentUpdates: ReadonlyArray<LinearUpdateCommentRequest>
	readonly commentDeletes: ReadonlyArray<LinearDeleteCommentRequest>
}

/**
 * Run one attempt through the Linear output processor over a recording `LinearApi`. `activityError`,
 * when given, is how Linear answers every activity; `deleteError` how it answers a comment delete.
 */
const run = (
	input: ProviderOutputAttempt,
	options: {
		readonly activityError?: LinearApiError
		readonly deleteError?: LinearApiError
		/** How Linear answers every comment create, update, and delete. */
		readonly commentError?: LinearApiError
	} = {},
) =>
	Effect.gen(function* () {
		const calls = yield* Ref.make<Calls>({
			activities: [],
			sessionUpdates: [],
			comments: [],
			commentUpdates: [],
			commentDeletes: [],
		})
		const record = <K extends keyof Calls>(key: K, value: Calls[K][number]) =>
			Ref.update(calls, (all) => ({ ...all, [key]: [...all[key], value] }))
		const commentAnswer = options.commentError === undefined ? Effect.void : Effect.fail(options.commentError)
		const api = Layer.mock(LinearApi, {
			createAgentActivity: (request) =>
				record('activities', request).pipe(
					Effect.andThen(
						options.activityError === undefined
							? Effect.succeed(
									LinearAgentActivityReceipt.make({
										activityId: request.activityId ?? LinearAgentActivityId.make('assigned-by-linear'),
										sessionId: request.sessionId,
									}),
								)
							: Effect.fail(options.activityError),
					),
				),
			updateAgentSession: (request) => record('sessionUpdates', request),
			createComment: (request) =>
				record('comments', request).pipe(
					Effect.andThen(commentAnswer),
					Effect.as(
						LinearComment.make({
							ref: commentRef,
							issue: LinearIssueRef.make({ organizationId: linearOrganizationId, teamId: null, issueId }),
							parentCommentId: null,
							content: request.content,
							author: null,
							files: [],
						}),
					),
				),
			updateComment: (request) =>
				record('commentUpdates', request).pipe(
					Effect.andThen(commentAnswer),
					Effect.as(
						LinearComment.make({
							ref: request.comment,
							issue: LinearIssueRef.make({ organizationId: linearOrganizationId, teamId: null, issueId }),
							parentCommentId: null,
							content: request.content,
							author: null,
							files: [],
						}),
					),
				),
			deleteComment: (request) =>
				record('commentDeletes', request).pipe(
					Effect.andThen(options.deleteError === undefined ? Effect.void : Effect.fail(options.deleteError)),
					Effect.andThen(commentAnswer),
				),
		})
		const processor = yield* makeLinearOutputProcessor({ namespace: 'linear-output-test', bot }).pipe(
			Effect.provide(api),
		)
		const result = yield* processor.process(input).pipe(
			Effect.match({
				onSuccess: (success) => ({ _tag: 'Success' as const, success }),
				onFailure: (failure) => ({ _tag: 'Failure' as const, failure }),
			}),
		)
		return { result, calls: yield* Ref.get(calls) }
	})

const activityReceipt = DeliveryOutputApplied.make({
	receipt: { _tag: 'LinearAgentActivity', activityId: idempotencyKey },
})

const linearError = (reason: LinearApiError['reason'], retryable: boolean, retryAfterMs?: number) =>
	LinearApiError.make(
		retryAfterMs === undefined
			? { operation: 'create_agent_activity', reason, retryable }
			: { operation: 'create_agent_activity', reason, retryable, retryAfterMs },
	)

const commentFault = {
	retryable: LinearApiError.make({ operation: 'create_comment', reason: 'unavailable', retryable: true }),
	final: LinearApiError.make({ operation: 'create_comment', reason: 'forbidden', retryable: false }),
	gone: LinearApiError.make({ operation: 'update_comment', reason: 'not_found', retryable: false }),
} as const

commentOutputScenarios({
	provider: 'Linear issue',
	optionBullet: '- ',
	run: (operation, options = {}) => {
		const base = attempt(operation, issueDestination)
		const input =
			options.futureVersion === true
				? ProviderOutputAttempt.make({ ...base, prepared: prepared(issueDestination, 2) })
				: base
		return run(input, options.fault === undefined ? {} : { commentError: commentFault[options.fault] }).pipe(
			Effect.map(({ result, calls }) => ({
				result: Match.valueTags(result, {
					Success: ({ success }) => Result.succeed(success),
					Failure: ({ failure }) => Result.fail(failure),
				}),
				shown: [
					...calls.comments.map(({ content }) => `post: ${content.markdown}`),
					...calls.commentUpdates.map(({ content }) => `edit: ${content.markdown}`),
					...calls.commentDeletes.map(() => 'delete'),
				],
			})),
		)
	},
})

describe('Linear output: Agent Session', () => {
	it.effect('posts one final activity for each outcome, with a default text when there is no Markdown', ({ expect }) =>
		Effect.gen(function* () {
			const cases = [
				[outcome(DeliveryOutcome.cases.Completed.make({}), 'Fixed.'), { type: 'Response', body: 'Fixed.' }],
				[outcome(DeliveryOutcome.cases.Completed.make({})), { type: 'Response', body: linearDefaultOutcomeText.Completed }],
				[outcome(DeliveryOutcome.cases.Failed.make({}), 'Stopped as requested.'), { type: 'Error', body: 'Stopped as requested.' }],
				[outcome(DeliveryOutcome.cases.Failed.make({})), { type: 'Error', body: linearDefaultOutcomeText.Failed }],
				[
					outcome(DeliveryOutcome.cases.AwaitingInput.make({ options: ['staging', 'production'] }), 'Where to?'),
					{ type: 'Elicitation', body: 'Where to?', options: ['staging', 'production'] },
				],
				[
					outcome(DeliveryOutcome.cases.AwaitingInput.make({})),
					{ type: 'Elicitation', body: linearDefaultOutcomeText.AwaitingInput },
				],
			] as const
			for (const [operation, expected] of cases) {
				const { result, calls } = yield* run(attempt(operation))
				expect(result).toEqual({ _tag: 'Success', success: activityReceipt })
				expect(calls.activities).toHaveLength(1)
				const [request] = calls.activities
				const { type, ...content } = expected
				expect(request).toEqual({
					organizationId: linearOrganizationId,
					sessionId,
					content: { _tag: type, ...content },
					ephemeral: false,
					activityId: idempotencyKey,
				})
			}
		}),
	)

	it.effect('shows Working as an ephemeral thought under the operation key, and applies Idle without a call', ({ expect }) =>
		Effect.gen(function* () {
			const working = yield* run(
				attempt({ _tag: 'SetActivity', activity: DeliveryActivity.cases.Working.make({ message: 'Running tests' }) }),
			)
			expect(working.result).toEqual({ _tag: 'Success', success: activityReceipt })
			expect(working.calls.activities).toEqual([
				{
					organizationId: linearOrganizationId,
					sessionId,
					content: { _tag: 'Thought', body: 'Running tests' },
					ephemeral: true,
					activityId: idempotencyKey,
				},
			])
			const idle = yield* run(attempt({ _tag: 'SetActivity', activity: DeliveryActivity.cases.Idle.make({}) }))
			expect(idle.result).toEqual({ _tag: 'Success', success: DeliveryOutputApplied.make({}) })
			expect(idle.calls.activities).toEqual([])
		}),
	)

	it.effect('posts a message as a lasting thought', ({ expect }) =>
		Effect.gen(function* () {
			const { result, calls } = yield* run(
				attempt({ _tag: 'CreateMessage', messageId: MessageId.make('summary'), markdown: 'Summary' }),
			)
			expect(result).toEqual({ _tag: 'Success', success: activityReceipt })
			expect(calls.activities[0]).toMatchObject({ content: { _tag: 'Thought', body: 'Summary' }, ephemeral: false })
		}),
	)

	it.effect('counts an activity Linear already has under the same ID as applied', ({ expect }) =>
		Effect.gen(function* () {
			const { result, calls } = yield* run(attempt(outcome(DeliveryOutcome.cases.Completed.make({}), 'Done')), {
				activityError: linearError('already_exists', false),
			})
			expect(result).toEqual({ _tag: 'Success', success: activityReceipt })
			expect(calls.activities).toHaveLength(1)
		}),
	)

	it.effect('sorts other Linear failures into retryable and final', ({ expect }) =>
		Effect.gen(function* () {
			const retryable = yield* run(attempt(outcome(DeliveryOutcome.cases.Completed.make({}))), {
				activityError: linearError('rate_limited', true, 2_000),
			})
			expect(retryable.result).toMatchObject({
				_tag: 'Failure',
				failure: { _tag: 'DeliveryOutputFailed', retryable: true, retryAfterMs: 2_000, safeCode: 'linear_activity_failed' },
			})
			const invalidId = yield* run(attempt(outcome(DeliveryOutcome.cases.Completed.make({}))), {
				activityError: linearError('rejected', false),
			})
			expect(invalidId.result).toMatchObject({
				_tag: 'Failure',
				failure: { retryable: false, safeCode: 'linear_activity_failed' },
			})
		}),
	)

	it.effect('refuses message update and delete, which a session cannot show', ({ expect }) =>
		Effect.gen(function* () {
			const messageId = MessageId.make('summary')
			const reference = { _tag: 'LinearAgentActivity', activityId: idempotencyKey }
			for (const operation of [
				{ _tag: 'UpdateMessage', messageId, markdown: 'x', reference },
				{ _tag: 'DeleteMessage', messageId, reference },
			] as const) {
				const { result, calls } = yield* run(attempt(operation))
				expect(result).toMatchObject({ _tag: 'Failure', failure: { retryable: false, safeCode: 'unsupported_operation' } })
				expect(calls.activities).toEqual([])
			}
			expect(linearSupportedOperations(sessionDestination)).not.toContain('UpdateMessage')
			expect(linearSupportedOperations(sessionDestination)).not.toContain('DeleteMessage')
			expect(linearSupportedOperations(sessionDestination)).toContain('SetActivity')
		}),
	)

	it.effect('adds a link to the session', ({ expect }) =>
		Effect.gen(function* () {
			const link = ExternalLink.make({ label: 'Run log', url: 'https://example.com/run/1' })
			const { result, calls } = yield* run(attempt({ _tag: 'AddExternalLink', link }))
			expect(result).toEqual({ _tag: 'Success', success: DeliveryOutputApplied.make({}) })
			expect(calls.sessionUpdates).toEqual([
				{
					organizationId: linearOrganizationId,
					sessionId,
					addedExternalUrls: [{ label: 'Run log', url: 'https://example.com/run/1' }],
				},
			])
		}),
	)

	it.effect('refuses a destination for another workspace or app user, or an unknown presentation version', ({ expect }) =>
		Effect.gen(function* () {
			const operation = outcome(DeliveryOutcome.cases.Completed.make({}))
			const otherOrganization = LinearAgentSessionDestination.make({
				...sessionDestination,
				organizationId: LinearOrganizationId.make('other-organization'),
			})
			const otherApp = LinearAgentSessionDestination.make({ ...sessionDestination, appUserId: LinearUserId.make('other-app') })
			for (const destination of [otherOrganization, otherApp]) {
				const { result, calls } = yield* run(attempt(operation, destination))
				expect(result).toMatchObject({
					_tag: 'Failure',
					failure: { retryable: false, safeCode: 'destination_identity_mismatch' },
				})
				expect(calls.activities).toEqual([])
			}
			const version = yield* run(ProviderOutputAttempt.make({ ...attempt(operation), prepared: prepared(sessionDestination, 2) }))
			expect(version.result).toMatchObject({ _tag: 'Failure', failure: { safeCode: 'unsupported_presentation_version' } })
		}),
	)
})

describe('Linear output: issue', () => {
	it.effect('comments a result with Markdown, listing any options, and applies one without Markdown silently', ({ expect }) =>
		Effect.gen(function* () {
			const asked = yield* run(
				attempt(outcome(DeliveryOutcome.cases.AwaitingInput.make({ options: ['staging', 'production'] }), 'Where to?'), issueDestination),
			)
			expect(asked.result).toEqual({ _tag: 'Success', success: DeliveryOutputApplied.make({ receipt: commentReceipt }) })
			expect(asked.calls.comments).toEqual([
				{
					issue: { organizationId: linearOrganizationId, teamId: null, issueId },
					content: LinearContent.make({ markdown: 'Where to?\n\n- staging\n- production' }),
				},
			])
			const silent = yield* run(attempt(outcome(DeliveryOutcome.cases.Completed.make({})), issueDestination))
			expect(silent.result).toEqual({ _tag: 'Success', success: DeliveryOutputApplied.make({}) })
			expect(silent.calls.comments).toEqual([])
			expect(silent.calls.activities).toEqual([])
		}),
	)

	it.effect('creates, edits, and removes a message as a comment, and counts a comment already gone as removed', ({ expect }) =>
		Effect.gen(function* () {
			const messageId = MessageId.make('progress')
			const created = yield* run(attempt({ _tag: 'CreateMessage', messageId, markdown: 'Running' }, issueDestination))
			expect(created.result).toEqual({ _tag: 'Success', success: DeliveryOutputApplied.make({ receipt: commentReceipt }) })
			const updated = yield* run(
				attempt({ _tag: 'UpdateMessage', messageId, markdown: 'Passed', reference: commentReceipt }, issueDestination),
			)
			expect(updated.calls.commentUpdates).toEqual([{ comment: commentRef, content: { markdown: 'Passed' } }])
			const removed = yield* run(attempt({ _tag: 'DeleteMessage', messageId, reference: commentReceipt }, issueDestination))
			expect(removed.calls.commentDeletes).toEqual([{ comment: commentRef }])
			const gone = yield* run(attempt({ _tag: 'DeleteMessage', messageId, reference: commentReceipt }, issueDestination), {
				deleteError: LinearApiError.make({ operation: 'delete_comment', reason: 'not_found', retryable: false }),
			})
			expect(gone.result).toEqual({ _tag: 'Success', success: DeliveryOutputApplied.make({}) })
			const badReference = yield* run(
				attempt({ _tag: 'DeleteMessage', messageId, reference: { something: 'else' } }, issueDestination),
			)
			expect(badReference.result).toMatchObject({ _tag: 'Failure', failure: { safeCode: 'message_reference_invalid' } })
		}),
	)

	it.effect('applies a link without a call, and refuses activity, which an issue cannot show', ({ expect }) =>
		Effect.gen(function* () {
			const link = ExternalLink.make({ label: 'Run log', url: 'https://example.com/run/1' })
			const linked = yield* run(attempt({ _tag: 'AddExternalLink', link }, issueDestination))
			expect(linked.result).toEqual({ _tag: 'Success', success: DeliveryOutputApplied.make({}) })
			expect(linked.calls.sessionUpdates).toEqual([])
			const activity = yield* run(
				attempt({ _tag: 'SetActivity', activity: DeliveryActivity.cases.Working.make({ message: 'x' }) }, issueDestination),
			)
			expect(activity.result).toMatchObject({ _tag: 'Failure', failure: { safeCode: 'unsupported_operation' } })
			expect(linearSupportedOperations(issueDestination)).not.toContain('SetActivity')
		}),
	)
})
