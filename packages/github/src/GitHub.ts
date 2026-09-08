import { Context, Effect, Layer, Schema } from 'effect'
import { HttpClient, HttpClientRequest } from 'effect/unstable/http'

import { GitHubCredentials } from './GitHubCredentials.js'
import { GitHubError } from './GitHubErrors.js'
import { GitHubCommentData, GitHubIssueData } from './GitHubEvents.js'
import { apiRequest, requestJson } from './GitHubHttp.js'
import {
	AddReactionInput,
	ListReactionsInput,
	RemoveReactionInput,
	GitHubReaction,
	GitHubReactionData,
	type GitHubReactionTarget,
} from './GitHubReaction.js'
import { GitHubCommentRef, GitHubDiscussionRef, GitHubIssueRef, GitHubRepository } from './GitHubResource.js'

export const GitHubIssue = Schema.Struct({ ref: GitHubIssueRef, data: GitHubIssueData })
export interface GitHubIssue extends Schema.Schema.Type<typeof GitHubIssue> {}
export const GitHubComment = Schema.Struct({ ref: GitHubCommentRef, data: GitHubCommentData })
export interface GitHubComment extends Schema.Schema.Type<typeof GitHubComment> {}
const CreateIssue = Schema.Struct({ repository: GitHubRepository, title: Schema.NonEmptyString, body: Schema.String })
const UpdateIssue = Schema.Struct({
	issue: GitHubIssueRef,
	title: Schema.optionalKey(Schema.NonEmptyString),
	body: Schema.optionalKey(Schema.String),
	state: Schema.optionalKey(Schema.Literals(['open', 'closed'])),
})
const CreateComment = Schema.Struct({ issue: GitHubDiscussionRef, body: Schema.NonEmptyString })
const UpdateComment = Schema.Struct({ comment: GitHubCommentRef, body: Schema.NonEmptyString })
const ListComments = Schema.Struct({
	issue: GitHubDiscussionRef,
	page: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
})

const repositoryPath = (repository: GitHubRepository) =>
	`/repos/${encodeURIComponent(repository.owner)}/${encodeURIComponent(repository.name)}`
const issuePath = (issue: GitHubDiscussionRef) => `${repositoryPath(issue.repository)}/issues/${issue.number}`
const parse = <S extends Schema.Top>(schema: S, input: S['Type']) =>
	Schema.decodeUnknownEffect(schema)(input).pipe(Effect.mapError(() => GitHubError.make({ reason: 'invalid_input' })))

const call = <S extends Schema.Top>(
	repository: GitHubRepository,
	method: 'GET' | 'POST' | 'PATCH' | 'DELETE',
	path: string,
	schema: S,
	body?: Schema.Json,
	statuses?: ReadonlyArray<number>,
) =>
	Effect.gen(function* () {
		const credentials = yield* GitHubCredentials
		const token = yield* credentials.token(repository)
		const base = apiRequest({ baseUrl: credentials.apiUrl, path, method }).pipe(
			HttpClientRequest.bearerToken(token),
		)
		const request =
			body === undefined
				? base
				: yield* HttpClientRequest.bodyJson(base, body).pipe(
						Effect.mapError(() => GitHubError.make({ reason: 'invalid_input' })),
					)
		return yield* requestJson(request, schema, statuses).pipe(
			Effect.tapErrorTag('GitHubError', (error) =>
				error.reason === 'authentication' ? credentials.invalidate(repository) : Effect.void,
			),
		)
	})
const issueResult = (repository: GitHubRepository, data: GitHubIssueData) =>
	data.pull_request === undefined
		? Effect.succeed(GitHubIssue.make({ ref: { kind: 'github.issue', repository, number: data.number }, data }))
		: Effect.fail(GitHubError.make({ reason: 'invalid_input' }))
const commentResult = (issue: GitHubDiscussionRef, data: GitHubCommentData) =>
	GitHubComment.make({ ref: { kind: 'github.issue-comment', issue, id: data.id }, data })

const checkDiscussion = Effect.fn('github.check_discussion')(function* (issue: GitHubDiscussionRef) {
	const data = yield* call(issue.repository, 'GET', issuePath(issue), GitHubIssueData)
	if (data.number !== issue.number || (data.pull_request === undefined) !== (issue.kind === 'github.issue'))
		return yield* GitHubError.make({ reason: 'invalid_input' })
})

const createIssue = Effect.fn('github.create_issue')(function* (input: typeof CreateIssue.Type) {
	const value = yield* parse(CreateIssue, input)
	return yield* issueResult(
		value.repository,
		yield* call(value.repository, 'POST', `${repositoryPath(value.repository)}/issues`, GitHubIssueData, {
			title: value.title,
			body: value.body,
		}),
	)
})
const getIssue = Effect.fn('github.get_issue')(function* (input: { readonly issue: GitHubIssueRef }) {
	const issue = yield* parse(GitHubIssueRef, input.issue)
	return yield* issueResult(issue.repository, yield* call(issue.repository, 'GET', issuePath(issue), GitHubIssueData))
})
const updateIssue = Effect.fn('github.update_issue')(function* (input: typeof UpdateIssue.Type) {
	const value = yield* parse(UpdateIssue, input)
	const { issue, ...body } = value
	yield* getIssue({ issue })
	return yield* issueResult(
		issue.repository,
		yield* call(issue.repository, 'PATCH', issuePath(issue), GitHubIssueData, body),
	)
})
const createComment = Effect.fn('github.create_comment')(function* (input: typeof CreateComment.Type) {
	const value = yield* parse(CreateComment, input)
	yield* checkDiscussion(value.issue)
	return commentResult(
		value.issue,
		yield* call(value.issue.repository, 'POST', `${issuePath(value.issue)}/comments`, GitHubCommentData, {
			body: value.body,
		}),
	)
})
const updateComment = Effect.fn('github.update_comment')(function* (input: typeof UpdateComment.Type) {
	const value = yield* parse(UpdateComment, input)
	yield* checkDiscussion(value.comment.issue)
	const existing = yield* call(
		value.comment.issue.repository,
		'GET',
		`${repositoryPath(value.comment.issue.repository)}/issues/comments/${value.comment.id}`,
		Schema.Struct({ id: Schema.Int, issue_url: Schema.String }),
	)
	const credentials = yield* GitHubCredentials
	if (
		existing.id !== value.comment.id ||
		existing.issue_url.toLowerCase() !== `${credentials.apiUrl}${issuePath(value.comment.issue)}`.toLowerCase()
	)
		return yield* GitHubError.make({ reason: 'invalid_input' })
	return commentResult(
		value.comment.issue,
		yield* call(
			value.comment.issue.repository,
			'PATCH',
			`${repositoryPath(value.comment.issue.repository)}/issues/comments/${value.comment.id}`,
			GitHubCommentData,
			{ body: value.body },
		),
	)
})
const listComments = Effect.fn('github.list_comments')(function* (input: typeof ListComments.Type) {
	const value = yield* parse(ListComments, input)
	yield* checkDiscussion(value.issue)
	return (yield* call(
		value.issue.repository,
		'GET',
		`${issuePath(value.issue)}/comments?per_page=100&page=${value.page ?? 1}`,
		Schema.Array(GitHubCommentData),
	)).map((data) => commentResult(value.issue, data))
})

const checkReactionTarget = Effect.fn('github.check_reaction_target')(function* (target: GitHubReactionTarget) {
	const issue = target.kind === 'github.issue-comment' ? target.issue : target
	const repository = issue.repository
	const actual = yield* call(repository, 'GET', repositoryPath(repository), Schema.Struct({ id: Schema.Int }))
	if (actual.id !== repository.id) return yield* GitHubError.make({ reason: 'invalid_input' })
	yield* checkDiscussion(issue)
	if (target.kind === 'github.issue-comment') {
		const path = `${repositoryPath(repository)}/issues/comments/${target.id}`
		const comment = yield* call(
			repository,
			'GET',
			path,
			Schema.Struct({ id: Schema.Int, issue_url: Schema.String }),
		)
		const credentials = yield* GitHubCredentials
		if (
			comment.id !== target.id ||
			comment.issue_url.toLowerCase() !== `${credentials.apiUrl}${issuePath(issue)}`.toLowerCase()
		)
			return yield* GitHubError.make({ reason: 'invalid_input' })
		return { repository, path: `${path}/reactions` }
	}
	return { repository, path: `${issuePath(issue)}/reactions` }
})
const reactionResult = (target: GitHubReactionTarget, data: GitHubReactionData): GitHubReaction => ({
	ref: { kind: 'github.reaction', target, id: data.id },
	data,
})
const addReaction = Effect.fn('github.add_reaction')(function* (input: AddReactionInput) {
	const value = yield* parse(AddReactionInput, input)
	const { repository, path } = yield* checkReactionTarget(value.target)
	const data = yield* call(
		repository,
		'POST',
		path,
		GitHubReactionData.check(Schema.makeFilter((data) => data.content === value.content)),
		{ content: value.content },
		[200, 201],
	)
	return reactionResult(value.target, data)
})
const listReactions = Effect.fn('github.list_reactions')(function* (input: ListReactionsInput) {
	const value = yield* parse(ListReactionsInput, input)
	const { repository, path } = yield* checkReactionTarget(value.target)
	const filter = value.content === undefined ? '' : `&content=${encodeURIComponent(value.content)}`
	const data = yield* call(
		repository,
		'GET',
		`${path}?per_page=${value.perPage}&page=${value.page}${filter}`,
		Schema.Array(GitHubReactionData).check(Schema.isMaxLength(value.perPage)),
		undefined,
		[200],
	)
	return data.map((entry) => reactionResult(value.target, entry))
})
const removeReaction = Effect.fn('github.remove_reaction')(function* (input: RemoveReactionInput) {
	const value = yield* parse(RemoveReactionInput, input)
	const { repository, path } = yield* checkReactionTarget(value.reaction.target)
	yield* call(repository, 'DELETE', `${path}/${value.reaction.id}`, Schema.Void, undefined, [204])
})

export class GitHub extends Context.Service<
	GitHub,
	{
		readonly addReaction: (input: AddReactionInput) => Effect.Effect<GitHubReaction, GitHubError>
		readonly listReactions: (input: ListReactionsInput) => Effect.Effect<ReadonlyArray<GitHubReaction>, GitHubError>
		readonly removeReaction: (input: RemoveReactionInput) => Effect.Effect<void, GitHubError>
		readonly createIssue: (input: typeof CreateIssue.Type) => Effect.Effect<GitHubIssue, GitHubError>
		readonly getIssue: (input: { readonly issue: GitHubIssueRef }) => Effect.Effect<GitHubIssue, GitHubError>
		readonly updateIssue: (input: typeof UpdateIssue.Type) => Effect.Effect<GitHubIssue, GitHubError>
		readonly createComment: (input: typeof CreateComment.Type) => Effect.Effect<GitHubComment, GitHubError>
		readonly updateComment: (input: typeof UpdateComment.Type) => Effect.Effect<GitHubComment, GitHubError>
		readonly listComments: (
			input: typeof ListComments.Type,
		) => Effect.Effect<ReadonlyArray<GitHubComment>, GitHubError>
	}
>()('github/GitHub') {
	static readonly layer = Layer.effect(
		GitHub,
		Effect.gen(function* () {
			const credentials = yield* GitHubCredentials
			const http = yield* HttpClient.HttpClient
			const provide = <A>(effect: Effect.Effect<A, GitHubError, GitHubCredentials | HttpClient.HttpClient>) =>
				effect.pipe(
					Effect.provideService(GitHubCredentials, credentials),
					Effect.provideService(HttpClient.HttpClient, http),
				)
			return GitHub.of({
				addReaction: (input) => provide(addReaction(input)),
				listReactions: (input) => provide(listReactions(input)),
				removeReaction: (input) => provide(removeReaction(input)),
				createIssue: (input) => provide(createIssue(input)),
				getIssue: (input) => provide(getIssue(input)),
				updateIssue: (input) => provide(updateIssue(input)),
				createComment: (input) => provide(createComment(input)),
				updateComment: (input) => provide(updateComment(input)),
				listComments: (input) => provide(listComments(input)),
			})
		}),
	)
}
