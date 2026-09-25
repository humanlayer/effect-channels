import { Match, Predicate, Schema } from 'effect'

import type { GitHubPostPullRequestReviewComment } from '../GitHubApi'
import type { GitHubId } from '../GitHubIdentity'
import {
	GitHubActionsJobInfo,
	type GitHubActionsJobRef,
	GitHubCheckAnnotation,
	GitHubCheckRunInfo,
	type GitHubCheckRunRef,
	type GitHubCommentRef,
	GitHubCommit,
	type GitHubIssueCommentRef,
	GitHubIssueInfo,
	type GitHubIssueRef,
	GitHubLabel,
	GitHubParticipant,
	GitHubPullRequestFile,
	GitHubPullRequestInfo,
	type GitHubPullRequestRef,
	type GitHubRepositoryRef,
	GitHubReview,
	GitHubReviewCommentRef,
	type GitHubReviewState,
} from '../GitHubModels'
import { GitHubActionsJob, GitHubCheckRun, GitHubIssueComment, GitHubReviewComment } from '../GitHubResources'
import * as Api from './GitHubApiSchemas'

export const repositoryPath = (ref: GitHubRepositoryRef) =>
	`/repos/${encodeURIComponent(ref.owner)}/${encodeURIComponent(ref.repository)}`
export const participant = (value: typeof Api.Participant.Type) => GitHubParticipant.make(value)
const participantOrNull = (value: typeof Api.Participant.Type | null) => {
	if (value === null) return null
	return participant(value)
}
export const issueInfo = (ref: GitHubIssueRef, value: typeof Api.Issue.Type) =>
	GitHubIssueInfo.make({
		ref,
		title: value.title,
		body: value.body,
		state: value.state,
		url: value.html_url,
		author: participant(value.user),
	})
export const pullRequestInfo = (ref: GitHubPullRequestRef, value: typeof Api.PullRequest.Type) =>
	GitHubPullRequestInfo.make({
		ref,
		title: value.title,
		body: value.body,
		state: value.state,
		url: value.html_url,
		author: participantOrNull(value.user),
		draft: value.draft,
		merged: value.merged,
		headRef: value.head.ref,
		headSha: value.head.sha,
		baseRef: value.base.ref,
		baseSha: value.base.sha,
	})
export const issueComment = (discussion: GitHubIssueCommentRef['discussion'], value: typeof Api.IssueComment.Type) =>
	GitHubIssueComment.make({
		ref: { discussion, id: value.id },
		body: value.body,
		url: value.html_url,
		author: participantOrNull(value.user),
	})
export const reviewState = (state: string): GitHubReviewState =>
	Match.value(state.toLowerCase()).pipe(
		Match.when('approved', () => 'approved' as const),
		Match.when('changes_requested', () => 'changes_requested' as const),
		Match.when('commented', () => 'commented' as const),
		Match.when('dismissed', () => 'dismissed' as const),
		Match.orElse(() => 'pending' as const),
	)
export const review = (pullRequest: GitHubPullRequestRef, value: typeof Api.Review.Type) =>
	GitHubReview.make({
		ref: { pullRequest, id: value.id, nodeId: value.node_id },
		body: value.body,
		author: participantOrNull(value.user),
		state: reviewState(value.state),
		commitId: value.commit_id,
		url: value.html_url,
	})
export const reviewComment = (pullRequest: GitHubPullRequestRef, value: typeof Api.ReviewComment.Type) => {
	const result: {
		ref: { pullRequest: GitHubPullRequestRef; id: GitHubId }
		nodeId: string
		body: string
		url: string
		author: ReturnType<typeof participant> | null
		reviewId: GitHubId | null
		path: string
		commitId: string
		originalCommitId: string
		diffHunk: string
		inReplyToId?: GitHubId | null
		line?: number | null
		startLine?: number | null
		side?: 'LEFT' | 'RIGHT'
	} = {
		ref: { pullRequest, id: value.id },
		nodeId: value.node_id,
		body: value.body,
		url: value.html_url,
		author: participantOrNull(value.user),
		reviewId: value.pull_request_review_id,
		path: value.path,
		commitId: value.commit_id,
		originalCommitId: value.original_commit_id,
		diffHunk: value.diff_hunk,
	}
	if (Predicate.isNotUndefined(value.in_reply_to_id)) result.inReplyToId = value.in_reply_to_id
	if (Predicate.isNotUndefined(value.line)) result.line = value.line
	if (Predicate.isNotUndefined(value.start_line)) result.startLine = value.start_line
	if (Predicate.isNotUndefined(value.side)) result.side = value.side
	return GitHubReviewComment.make(result)
}
export const pullRequestFile = (value: typeof Api.PullRequestFile.Type) => {
	const status = Match.value(value.status).pipe(
		Match.when('removed', () => 'deleted' as const),
		Match.orElse((other) => other),
	)
	const result: {
		sha: string | null
		filename: string
		previousFilename?: string
		status: typeof status
		additions: number
		deletions: number
		changes: number
		blobUrl: string | null
		rawUrl: string | null
		contentsUrl: string
		patch?: string
	} = {
		sha: value.sha,
		filename: value.filename,
		status,
		additions: value.additions,
		deletions: value.deletions,
		changes: value.changes,
		blobUrl: value.blob_url,
		rawUrl: value.raw_url,
		contentsUrl: value.contents_url,
	}
	if (Predicate.isNotUndefined(value.previous_filename)) result.previousFilename = value.previous_filename
	if (Predicate.isNotUndefined(value.patch)) result.patch = value.patch
	return GitHubPullRequestFile.make(result)
}
const commitParticipant = (value: typeof Api.CommitParticipant.Type) => {
	if (value === null || !Schema.is(Api.Participant)(value)) return null
	return participant(value)
}
export const commit = (value: typeof Api.Commit.Type) =>
	GitHubCommit.make({
		sha: value.sha,
		message: value.commit.message,
		apiUrl: value.url,
		url: value.html_url,
		author: commitParticipant(value.author),
		committer: commitParticipant(value.committer),
	})
export const label = (value: typeof Api.Label.Type) => {
	const result: { id?: GitHubId; name: string; color: string; description: string | null } = {
		name: value.name,
		color: value.color,
		description: value.description,
	}
	if (Predicate.isNotUndefined(value.id)) result.id = value.id
	return GitHubLabel.make(result)
}
export const checkRunInfo = (ref: GitHubCheckRunRef, value: typeof Api.CheckRun.Type) =>
	GitHubCheckRunInfo.make({
		ref,
		name: value.name,
		headSha: value.head_sha,
		status: value.status,
		conclusion: value.conclusion,
		startedAt: value.started_at,
		completedAt: value.completed_at,
		apiUrl: value.url,
		url: value.html_url,
		detailsUrl: value.details_url,
		checkSuiteId: value.check_suite?.id ?? null,
		outputTitle: value.output.title,
		outputSummary: value.output.summary,
		outputText: value.output.text,
		annotationCount: value.output.annotations_count,
	})
export const checkRun = (repository: GitHubRepositoryRef, value: typeof Api.CheckRun.Type) =>
	GitHubCheckRun.make({
		ref: {
			installationId: repository.installationId,
			repositoryId: repository.repositoryId,
			owner: repository.owner,
			repository: repository.repository,
			id: value.id,
		},
	})
export const checkAnnotation = (value: typeof Api.CheckAnnotation.Type) =>
	GitHubCheckAnnotation.make({
		path: value.path,
		startLine: value.start_line,
		endLine: value.end_line,
		startColumn: value.start_column,
		endColumn: value.end_column,
		level: value.annotation_level,
		title: value.title,
		message: value.message,
		rawDetails: value.raw_details,
		blobUrl: value.blob_href,
	})
export const actionsJobInfo = (ref: GitHubActionsJobRef, value: typeof Api.ActionsJob.Type) => {
	const result: {
		ref: GitHubActionsJobRef
		runId: GitHubId
		name: string
		status: typeof value.status
		conclusion: typeof value.conclusion
		headSha: string
		apiUrl: string
		url: string | null
		startedAt: string | null
		completedAt: string | null
		checkRunUrl: string
		workflowName?: string
		headBranch?: string | null
		steps: ReadonlyArray<{
			name: string
			status: typeof value.status
			conclusion: typeof value.conclusion
			number: number
			startedAt: string | null
			completedAt: string | null
		}>
	} = {
		ref,
		runId: value.run_id,
		name: value.name,
		status: value.status,
		conclusion: value.conclusion,
		headSha: value.head_sha,
		apiUrl: value.url,
		url: value.html_url,
		startedAt: value.started_at,
		completedAt: value.completed_at,
		checkRunUrl: value.check_run_url,
		steps: (value.steps ?? []).map((step) => ({
			name: step.name,
			status: step.status,
			conclusion: step.conclusion,
			number: step.number,
			startedAt: step.started_at ?? null,
			completedAt: step.completed_at ?? null,
		})),
	}
	if (Predicate.isNotUndefined(value.workflow_name) && value.workflow_name !== null)
		result.workflowName = value.workflow_name
	if (Predicate.isNotUndefined(value.head_branch)) result.headBranch = value.head_branch
	return GitHubActionsJobInfo.make(result)
}
export const actionsJob = (repository: GitHubRepositoryRef, value: typeof Api.ActionsJob.Type) =>
	GitHubActionsJob.make({
		ref: {
			installationId: repository.installationId,
			repositoryId: repository.repositoryId,
			owner: repository.owner,
			repository: repository.repository,
			id: value.id,
		},
	})
export const sameUrl = (left: string, right: string) => left.replace(/\/$/, '') === right.replace(/\/$/, '')
export const reviewCommentLocationBody = (location: GitHubPostPullRequestReviewComment['location']) =>
	Match.value(location).pipe(
		Match.tagsExhaustive({
			Line: ({ line, side }) => ({ line, side }),
			Range: ({ startLine, startSide, line, side }) => ({
				start_line: startLine,
				start_side: startSide,
				line,
				side,
			}),
			File: () => ({ subject_type: 'file' as const }),
		}),
	)
export const commentRepository = (comment: GitHubCommentRef) =>
	Schema.is(GitHubReviewCommentRef)(comment)
		? comment.pullRequest
		: Match.value(comment.discussion).pipe(
				Match.tagsExhaustive({ Issue: ({ ref }) => ref, PullRequest: ({ ref }) => ref }),
			)
export const commentPath = (comment: GitHubCommentRef) =>
	Schema.is(GitHubReviewCommentRef)(comment)
		? `${repositoryPath(comment.pullRequest)}/pulls/comments/${comment.id}`
		: `${repositoryPath(commentRepository(comment))}/issues/comments/${comment.id}`
