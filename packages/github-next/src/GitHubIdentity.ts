import { Schema } from 'effect'

export const GitHubId = Schema.Int.check(
	Schema.isGreaterThan(0),
	Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
).pipe(Schema.brand('GitHubId'))
export type GitHubId = typeof GitHubId.Type

export const githubDiscussionResourceId = (input: {
	readonly repositoryId: GitHubId
	readonly kind: 'issue' | 'pull-request'
	readonly number: GitHubId
}) => ['github', 'v1', String(input.repositoryId), input.kind, String(input.number)].map(encodeURIComponent).join(':')
