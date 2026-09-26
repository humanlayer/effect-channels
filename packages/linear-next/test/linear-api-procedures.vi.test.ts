import { describe, it } from '@effect/vitest'
import { Effect, Layer, Match, Redacted, Schema } from 'effect'
import { HttpClient, HttpClientResponse } from 'effect/unstable/http'

import { CreateAttachmentVariables } from '../src/api/CreateAttachment'
import { CreateCommentVariables } from '../src/api/CreateComment'
import { CreateReactionVariables } from '../src/api/CreateReaction'
import { DeleteAttachmentVariables } from '../src/api/DeleteAttachment'
import { DeleteCommentVariables } from '../src/api/DeleteComment'
import { DeleteReactionVariables } from '../src/api/DeleteReaction'
import { getIssue, GetIssueVariables } from '../src/api/GetIssue'
import { GetUserVariables } from '../src/api/GetUser'
import { narrowLinearProviderErrors } from '../src/api/LinearApiErrors'
import { LinearHttpClient, makeFixedCredentialLinearHttpClient } from '../src/api/LinearHttpClient'
import { ListIssueAttachmentsVariables } from '../src/api/ListIssueAttachments'
import { ListIssueCommentsVariables } from '../src/api/ListIssueComments'
import {
	createAttachment,
	createComment,
	createReaction,
	deleteAttachment,
	deleteComment,
	deleteReaction,
	getUser,
	listIssueAttachments,
	listIssueComments,
	updateAttachment,
	updateComment,
	updateIssue,
} from '../src/api/Operations'
import { UpdateAttachmentVariables } from '../src/api/UpdateAttachment'
import { UpdateCommentVariables } from '../src/api/UpdateComment'
import { UpdateIssueVariables } from '../src/api/UpdateIssue'
import { LinearReactionTarget, LinearUpdateAttachmentInput } from '../src/LinearApi'
import {
	LinearAttachmentId,
	LinearCommentId,
	LinearIssueLabelId,
	LinearReactionId,
	LinearUserId,
} from '../src/LinearIdentity'
import { decodeGraphqlRequest, issue, issueJson } from './api-test-fixtures'

const decodeProcedureRequest = decodeGraphqlRequest(
	Schema.Union([
		GetUserVariables,
		ListIssueCommentsVariables,
		ListIssueAttachmentsVariables,
		CreateCommentVariables,
		UpdateCommentVariables,
		DeleteCommentVariables,
		CreateReactionVariables,
		DeleteReactionVariables,
		CreateAttachmentVariables,
		DeleteAttachmentVariables,
	]),
)

const procedureLayer = (http: HttpClient.HttpClient) =>
	Layer.succeed(
		LinearHttpClient,
		makeFixedCredentialLinearHttpClient(http, issue.organizationId, Redacted.make('test-token')),
	)

describe('Linear GraphQL procedures', () => {
	it.effect('owns its document, variables, decoding, and domain projection', ({ expect }) =>
		Effect.gen(function* () {
			const http = HttpClient.make((request) =>
				Effect.gen(function* () {
					const body = yield* decodeGraphqlRequest(GetIssueVariables)(request)
					expect(body.query).toContain('query LinearIssue')
					expect(body.variables).toEqual({ id: issue.issueId })
					return HttpClientResponse.fromWeb(request, Response.json({ data: { issue: issueJson } }))
				}),
			)
			const result = yield* getIssue({ issue }).pipe(Effect.provide(procedureLayer(http)))
			expect(result.identifier).toBe('CORE-1')
		}),
	)

	it.effect('uses schema-exact issue and attachment update variables and projects responses', ({ expect }) =>
		Effect.gen(function* () {
			yield* Schema.decodeUnknownEffect(LinearUpdateAttachmentInput)({ url: 'https://invalid.example' }).pipe(
				Effect.flip,
			)
			expect(yield* Schema.decodeEffect(LinearUpdateAttachmentInput)({ title: 'Renamed' })).toEqual({
				title: 'Renamed',
			})
			let call = 0
			const http = HttpClient.make((request) =>
				Effect.gen(function* () {
					call += 1
					if (call === 1) {
						const body = yield* decodeGraphqlRequest(UpdateIssueVariables)(request)
						expect(body.query).toContain('mutation LinearIssueUpdate')
						expect(body.variables).toEqual({
							id: issue.issueId,
							input: { priority: 3, addLabelIds: ['label-1'], removeLabelIds: ['label-2'] },
						})
						return HttpClientResponse.fromWeb(
							request,
							Response.json({
								data: { issueUpdate: { success: true, issue: { ...issueJson, priority: 3 } } },
							}),
						)
					}
					const body = yield* decodeGraphqlRequest(UpdateAttachmentVariables)(request)
					expect(body.query).toContain('$input: AttachmentUpdateInput!')
					expect(body.variables).toEqual({ id: 'attachment-1', input: { title: 'Renamed', subtitle: null } })
					return HttpClientResponse.fromWeb(
						request,
						Response.json({
							data: {
								attachmentUpdate: {
									success: true,
									attachment: {
										id: 'attachment-1',
										title: 'Renamed',
										subtitle: null,
										url: 'https://example.com',
										metadata: { source: 'test' },
									},
								},
							},
						}),
					)
				}),
			)
			const layer = procedureLayer(http)
			const updated = yield* updateIssue({
				issue,
				update: {
					priority: 3,
					addLabelIds: [LinearIssueLabelId.make('label-1')],
					removeLabelIds: [LinearIssueLabelId.make('label-2')],
				},
			}).pipe(Effect.provide(layer))
			expect(updated.priority).toBe(3)
			const attachment = yield* updateAttachment({
				attachment: { issue, attachmentId: LinearAttachmentId.make('attachment-1') },
				input: { title: 'Renamed', subtitle: null },
			}).pipe(Effect.provide(layer))
			expect(attachment.title).toBe('Renamed')
			expect(attachment.url).toBe('https://example.com')
		}),
	)

	it.effect('classifies GraphQL errors in HTTP 400 responses before generic HTTP handling', ({ expect }) =>
		Effect.gen(function* () {
			const cases = [
				['FORBIDDEN', 'forbidden'],
				['BAD_USER_INPUT', 'validation'],
				['ENTITY_NOT_FOUND', 'not_found'],
			] as const
			for (const [code, reason] of cases) {
				const http = HttpClient.make((request) =>
					Effect.succeed(
						HttpClientResponse.fromWeb(
							request,
							Response.json(
								{ errors: [{ message: `safe-${code}`, extensions: { code } }] },
								{ status: 400 },
							),
						),
					),
				)
				const error = yield* getIssue({ issue }).pipe(
					Effect.provide(procedureLayer(http)),
					narrowLinearProviderErrors,
					Effect.flip,
				)
				expect(error.reason).toBe(reason)
				expect(error.status).toBe(400)
				expect(error.retryable).toBe(false)
			}
			const rateLimited = HttpClient.make((request) =>
				Effect.succeed(
					HttpClientResponse.fromWeb(
						request,
						Response.json(
							{
								errors: [
									{
										message: 'slow down',
										extensions: { code: 'RATELIMITED', statusCode: 429, retryAfterMs: 123 },
									},
								],
							},
							{ status: 400 },
						),
					),
				),
			)
			const rateError = yield* getIssue({ issue }).pipe(
				Effect.provide(procedureLayer(rateLimited)),
				narrowLinearProviderErrors,
				Effect.flip,
			)
			expect(rateError.reason).toBe('rate_limited')
			expect(rateError.status).toBe(429)
			expect(rateError.retryable).toBe(true)
			expect(rateError.retryAfterMs).toBe(123)
		}),
	)

	it.effect('fails a decoded mutation whose success flag is false', ({ expect }) =>
		Effect.gen(function* () {
			const http = HttpClient.make((request) =>
				Effect.succeed(
					HttpClientResponse.fromWeb(
						request,
						Response.json({
							data: { issueUpdate: { success: false, issue: null } },
						}),
					),
				),
			)
			const error = yield* updateIssue({ issue, update: { priority: 3 } }).pipe(
				Effect.provide(procedureLayer(http)),
				narrowLinearProviderErrors,
				Effect.flip,
			)
			expect(error.reason).toBe('rejected')
			expect(error.retryable).toBe(false)
		}),
	)

	it.effect('owns exact comment, reaction, attachment, and user variables and projections', ({ expect }) =>
		Effect.gen(function* () {
			const seen: string[] = []
			const http = HttpClient.make((request) =>
				Effect.gen(function* () {
					const body = yield* decodeProcedureRequest(request)
					const operation = /(?:query|mutation) (Linear\w+)/.exec(body.query)?.[1]
					if (operation === undefined) return yield* Effect.die('missing operation name')
					seen.push(operation)
					const response = yield* Match.value(operation).pipe(
						Match.when('LinearUser', () => {
							expect(body.variables).toEqual({ id: 'user-1', teamId: 'team-1' })
							return Effect.succeed(
								Response.json({
									data: {
										user: {
											id: 'user-1',
											name: 'User',
											email: null,
											active: true,
											app: false,
											isAssignable: true,
											canAccessAnyPublicTeam: false,
											teams: { nodes: [{ id: issue.teamId }] },
										},
									},
								}),
							)
						}),
						Match.when('LinearIssueComments', () => {
							expect(body.variables).toEqual({ id: issue.issueId })
							return Effect.succeed(
								Response.json({
									data: {
										issue: {
											comments: {
												nodes: [{ id: 'comment-1', body: 'body', parent: null, user: null }],
												pageInfo: { endCursor: null, hasNextPage: false },
											},
										},
									},
								}),
							)
						}),
						Match.when('LinearIssueAttachments', () =>
							Effect.succeed(
								Response.json({
									data: {
										issue: {
											attachments: {
												nodes: [
													{
														id: 'attachment-1',
														title: 'Card',
														subtitle: null,
														url: 'https://example.com',
													},
												],
												pageInfo: { endCursor: null, hasNextPage: false },
											},
										},
									},
								}),
							),
						),
						Match.when('LinearCommentCreate', () => {
							expect(body.variables).toEqual({
								input: { issueId: issue.issueId, body: 'reply', parentId: 'comment-1' },
							})
							return Effect.succeed(
								Response.json({
									data: {
										commentCreate: {
											success: true,
											comment: {
												id: 'comment-2',
												body: 'reply',
												parent: { id: 'comment-1' },
												user: null,
											},
										},
									},
								}),
							)
						}),
						Match.when('LinearCommentUpdate', () => {
							expect(body.variables).toEqual({ id: 'comment-2', input: { body: 'edited' } })
							return Effect.succeed(
								Response.json({
									data: {
										commentUpdate: {
											success: true,
											comment: {
												id: 'comment-2',
												body: 'edited',
												parent: { id: 'comment-1' },
												user: null,
											},
										},
									},
								}),
							)
						}),
						Match.when('LinearCommentDelete', () =>
							Effect.succeed(Response.json({ data: { commentDelete: { success: true } } })),
						),
						Match.when('LinearReactionCreate', () => {
							expect(body.variables).toEqual({ input: { emoji: 'eyes', commentId: 'comment-2' } })
							return Effect.succeed(
								Response.json({
									data: {
										reactionCreate: {
											success: true,
											reaction: { id: 'reaction-1', emoji: 'eyes', user: null },
										},
									},
								}),
							)
						}),
						Match.when('LinearReactionDelete', () =>
							Effect.succeed(Response.json({ data: { reactionDelete: { success: true } } })),
						),
						Match.when('LinearAttachmentCreate', () => {
							expect(body.variables).toEqual({
								input: { issueId: issue.issueId, url: 'https://example.com', title: 'Card' },
							})
							return Effect.succeed(
								Response.json({
									data: {
										attachmentCreate: {
											success: true,
											attachment: {
												id: 'attachment-2',
												title: 'Card',
												subtitle: null,
												url: 'https://example.com',
											},
										},
									},
								}),
							)
						}),
						Match.when('LinearAttachmentDelete', () =>
							Effect.succeed(Response.json({ data: { attachmentDelete: { success: true } } })),
						),
						Match.orElse(() => Effect.die(`unexpected ${operation}`)),
					)
					return HttpClientResponse.fromWeb(request, response)
				}),
			)
			const layer = procedureLayer(http)
			const commentRef = { ...issue, commentId: LinearCommentId.make('comment-1') }
			const resolvedUser = yield* getUser({ issue, userId: LinearUserId.make('user-1') }).pipe(
				Effect.provide(layer),
			)
			const comments = yield* listIssueComments({ issue }).pipe(Effect.provide(layer))
			const attachments = yield* listIssueAttachments({ issue }).pipe(Effect.provide(layer))
			const reply = yield* createComment({
				issue,
				parentId: commentRef.commentId,
				content: { markdown: 'reply' },
			}).pipe(Effect.provide(layer))
			const edited = yield* updateComment({
				comment: reply.ref,
				content: { markdown: 'edited' },
			}).pipe(Effect.provide(layer))
			yield* deleteComment({ comment: edited.ref }).pipe(Effect.provide(layer))
			const reaction = yield* createReaction({
				target: LinearReactionTarget.cases.Comment.make({ comment: edited.ref }),
				emoji: 'eyes',
			}).pipe(Effect.provide(layer))
			yield* deleteReaction({ issue, reactionId: LinearReactionId.make('reaction-1') }).pipe(
				Effect.provide(layer),
			)
			const createdAttachment = yield* createAttachment({
				issue,
				input: { url: 'https://example.com', title: 'Card' },
			}).pipe(Effect.provide(layer))
			yield* deleteAttachment({ attachment: createdAttachment.ref }).pipe(Effect.provide(layer))
			expect(resolvedUser.teamIds).toEqual([issue.teamId])
			expect(comments[0]?.ref.commentId).toBe('comment-1')
			expect(attachments[0]?.ref.attachmentId).toBe('attachment-1')
			expect(reply.parentCommentId).toBe('comment-1')
			expect(reaction.ref.reactionId).toBe('reaction-1')
			expect(seen).toEqual([
				'LinearUser',
				'LinearIssueComments',
				'LinearIssueAttachments',
				'LinearCommentCreate',
				'LinearCommentUpdate',
				'LinearCommentDelete',
				'LinearReactionCreate',
				'LinearReactionDelete',
				'LinearAttachmentCreate',
				'LinearAttachmentDelete',
			])
		}),
	)
})
