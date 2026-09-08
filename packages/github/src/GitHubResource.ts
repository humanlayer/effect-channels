import { Schema } from 'effect'

export const GitHubId = Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER))
const RepositorySegment = Schema.NonEmptyString.check(
	Schema.isPattern(/^[a-zA-Z0-9_.-]+$/),
	Schema.makeFilter((value) => value !== '.' && value !== '..'),
)
export const GitHubRepository = Schema.Struct({
	kind: Schema.Literal('github.repository'),
	installationId: GitHubId,
	id: GitHubId,
	owner: RepositorySegment,
	name: RepositorySegment,
})
export interface GitHubRepository extends Schema.Schema.Type<typeof GitHubRepository> {}

export const GitHubIssueRef = Schema.Struct({
	kind: Schema.Literal('github.issue'),
	repository: GitHubRepository,
	number: GitHubId,
})
export interface GitHubIssueRef extends Schema.Schema.Type<typeof GitHubIssueRef> {}

export const GitHubPullRequestRef = Schema.Struct({
	kind: Schema.Literal('github.pull-request'),
	repository: GitHubRepository,
	number: GitHubId,
})
export interface GitHubPullRequestRef extends Schema.Schema.Type<typeof GitHubPullRequestRef> {}
export const GitHubDiscussionRef = Schema.Union([GitHubIssueRef, GitHubPullRequestRef])
export type GitHubDiscussionRef = typeof GitHubDiscussionRef.Type

export const GitHubCommentRef = Schema.Struct({
	kind: Schema.Literal('github.issue-comment'),
	issue: GitHubDiscussionRef,
	id: GitHubId,
})
export interface GitHubCommentRef extends Schema.Schema.Type<typeof GitHubCommentRef> {}

export const issueResourceKey = (issue: GitHubDiscussionRef) =>
	JSON.stringify([issue.repository.id, issue.kind === 'github.issue' ? 'issue' : 'pull-request', issue.number])
