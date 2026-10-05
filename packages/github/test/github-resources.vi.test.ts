import { describe, it } from '@effect/vitest'
import { Effect, Layer, Queue } from 'effect'

import { GitHubApi } from '../src/GitHubApi'
import { GitHubId } from '../src/GitHubIdentity'
import {
	GitHubActionsJobInfo,
	GitHubActionsJobRef,
	GitHubCheckAnnotation,
	GitHubCheckRunInfo,
	GitHubCheckRunRef,
	GitHubCommit,
	GitHubContent,
	GitHubIssueInfo,
	GitHubIssueRef,
	GitHubLabel,
	GitHubMergeResult,
	GitHubPullRequestFile,
	GitHubPullRequestInfo,
	GitHubPullRequestRef,
} from '../src/GitHubModels'
import {
	GitHubActionsJob,
	GitHubCheckRun,
	GitHubIssue,
	GitHubPullRequest,
	GitHubReviewComment,
} from '../src/GitHubResources'

const issueRef = GitHubIssueRef.make({
	installationId: GitHubId.make(100),
	repositoryId: GitHubId.make(200),
	owner: 'humanlayer',
	repository: 'channels',
	number: GitHubId.make(42),
})

const pullRequestRef = GitHubPullRequestRef.make({ ...issueRef, number: GitHubId.make(43) })

const checkRunRef = GitHubCheckRunRef.make({
	installationId: issueRef.installationId,
	repositoryId: issueRef.repositoryId,
	owner: issueRef.owner,
	repository: issueRef.repository,
	id: GitHubId.make(700),
})

const jobRef = GitHubActionsJobRef.make({
	installationId: issueRef.installationId,
	repositoryId: issueRef.repositoryId,
	owner: issueRef.owner,
	repository: issueRef.repository,
	id: GitHubId.make(900),
})

const participant = { id: GitHubId.make(999), login: 'agent[bot]', type: 'Bot' }

const issueInfo = GitHubIssueInfo.make({
	ref: issueRef,
	title: 'Issue',
	body: null,
	state: 'closed',
	url: 'https://github.test/issues/42',
	author: participant,
})

const pullRequestInfo = GitHubPullRequestInfo.make({
	ref: pullRequestRef,
	title: 'Pull request',
	body: null,
	state: 'open',
	url: 'https://github.test/pull/43',
	author: participant,
	draft: false,
	merged: false,
	headRef: 'feature',
	headSha: 'head-sha',
	baseRef: 'main',
	baseSha: 'base-sha',
})

const labels = [GitHubLabel.make({ id: GitHubId.make(1), name: 'bug', color: 'd73a4a', description: null })]

const files = [
	GitHubPullRequestFile.make({
		sha: 'file-sha',
		filename: 'src/index.ts',
		status: 'modified',
		additions: 2,
		deletions: 1,
		changes: 3,
		blobUrl: 'https://github.test/blob/file-sha/src/index.ts',
		rawUrl: 'https://github.test/raw/file-sha/src/index.ts',
		contentsUrl: 'https://api.github.test/contents/src/index.ts',
		patch: '@@ -1 +1 @@',
	}),
]

const commits = [
	GitHubCommit.make({
		sha: 'commit-sha',
		message: 'Implement it',
		apiUrl: 'https://api.github.test/commits/commit-sha',
		url: 'https://github.test/commit/commit-sha',
		author: participant,
		committer: participant,
	}),
]

const reviewComment = GitHubReviewComment.make({
	ref: { pullRequest: pullRequestRef, id: GitHubId.make(500) },
	nodeId: 'PRRC_500',
	body: 'Review body',
	url: 'https://github.test/pull/43#discussion_r500',
	author: participant,
	reviewId: GitHubId.make(501),
	path: 'src/index.ts',
	commitId: 'head-sha',
	originalCommitId: 'base-sha',
	diffHunk: '@@ -1 +1 @@',
	line: 10,
	startLine: null,
	side: 'RIGHT',
})

const mergeResult = GitHubMergeResult.make({ merged: true, sha: 'merge-sha', message: 'Merged' })
const checkRuns = [GitHubCheckRun.make({ ref: checkRunRef })]

const checkRunInfo = GitHubCheckRunInfo.make({
	ref: checkRunRef,
	name: 'build',
	headSha: 'head-sha',
	status: 'completed',
	conclusion: 'failure',
	startedAt: '2025-01-01T00:00:00Z',
	completedAt: '2025-01-01T00:01:00Z',
	apiUrl: 'https://api.github.test/check-runs/700',
	url: 'https://github.test/runs/700',
	detailsUrl: 'https://ci.test/build/700',
	checkSuiteId: GitHubId.make(800),
	outputTitle: 'Failed',
	outputSummary: 'One failure',
	outputText: 'Details',
	annotationCount: 1,
})

const annotations = [
	GitHubCheckAnnotation.make({
		path: 'src/index.ts',
		startLine: 5,
		endLine: 5,
		startColumn: null,
		endColumn: null,
		level: 'failure',
		title: 'Type error',
		message: 'Unknown property',
		rawDetails: null,
		blobUrl: 'https://github.test/blob/head-sha/src/index.ts',
	}),
]

const job = GitHubActionsJob.make({ ref: jobRef })

const jobInfo = GitHubActionsJobInfo.make({
	ref: jobRef,
	runId: GitHubId.make(850),
	name: 'test',
	status: 'completed',
	conclusion: 'failure',
	headSha: 'head-sha',
	apiUrl: 'https://api.github.test/actions/jobs/900',
	url: 'https://github.test/actions/jobs/900',
	startedAt: '2025-01-01T00:00:00Z',
	completedAt: '2025-01-01T00:01:00Z',
	checkRunUrl: 'https://api.github.test/check-runs/700',
	workflowName: 'CI',
	headBranch: 'feature',
	steps: [],
})

describe('GitHub resource capability delegation', () => {
	it.effect('delegates issue close, reopen, and every label operation to GitHubApi', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<unknown>()
			const layer = Layer.mock(GitHubApi, {
				closeIssue: (input) =>
					Queue.offer(calls, { operation: 'closeIssue', input }).pipe(Effect.as(issueInfo)),
				reopenIssue: (input) =>
					Queue.offer(calls, { operation: 'reopenIssue', input }).pipe(Effect.as(issueInfo)),
				listIssueLabels: (input) =>
					Queue.offer(calls, { operation: 'listIssueLabels', input }).pipe(Effect.as(labels)),
				addIssueLabels: (input) =>
					Queue.offer(calls, { operation: 'addIssueLabels', input }).pipe(Effect.as(labels)),
				setIssueLabels: (input) =>
					Queue.offer(calls, { operation: 'setIssueLabels', input }).pipe(Effect.as(labels)),
				removeIssueLabel: (input) =>
					Queue.offer(calls, { operation: 'removeIssueLabel', input }).pipe(Effect.as(labels)),
				removeAllIssueLabels: (input) =>
					Queue.offer(calls, { operation: 'removeAllIssueLabels', input }).pipe(Effect.asVoid),
			})
			const issue = GitHubIssue.make({ ref: issueRef, mailboxKey: 'issue-mailbox' })

			const results = yield* Effect.gen(function* () {
				return {
					closed: yield* issue.close('not_planned'),
					reopened: yield* issue.reopen(),
					listed: yield* issue.listLabels(),
					added: yield* issue.addLabels(['bug']),
					set: yield* issue.setLabels(['bug', 'urgent']),
					removed: yield* issue.removeLabel('needs review'),
					removeAll: yield* issue.removeAllLabels(),
				}
			}).pipe(Effect.provide(layer))

			expect(results.closed).toBe(issueInfo)
			expect(results.reopened).toBe(issueInfo)
			expect(results.listed).toBe(labels)
			expect(results.added).toBe(labels)
			expect(results.set).toBe(labels)
			expect(results.removed).toBe(labels)
			expect(results.removeAll).toBeUndefined()
			expect(Array.from(yield* Queue.takeAll(calls))).toEqual([
				{ operation: 'closeIssue', input: { issue: issueRef, reason: 'not_planned' } },
				{ operation: 'reopenIssue', input: { issue: issueRef } },
				{ operation: 'listIssueLabels', input: { issue: issueRef } },
				{ operation: 'addIssueLabels', input: { issue: issueRef, labels: ['bug'] } },
				{ operation: 'setIssueLabels', input: { issue: issueRef, labels: ['bug', 'urgent'] } },
				{ operation: 'removeIssueLabel', input: { issue: issueRef, label: 'needs review' } },
				{ operation: 'removeAllIssueLabels', input: { issue: issueRef } },
			])
		}),
	)

	it.effect('delegates every new pull request capability and resolves current-head checks in order', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<unknown>()
			const layer = Layer.mock(GitHubApi, {
				postPullRequestReviewComment: (input) =>
					Queue.offer(calls, { operation: 'postPullRequestReviewComment', input }).pipe(
						Effect.as(reviewComment),
					),
				listPullRequestFiles: (input) =>
					Queue.offer(calls, { operation: 'listPullRequestFiles', input }).pipe(Effect.as(files)),
				fetchPullRequestDiff: (input) =>
					Queue.offer(calls, { operation: 'fetchPullRequestDiff', input }).pipe(Effect.as('complete diff')),
				listPullRequestCommits: (input) =>
					Queue.offer(calls, { operation: 'listPullRequestCommits', input }).pipe(Effect.as(commits)),
				listPullRequestLabels: (input) =>
					Queue.offer(calls, { operation: 'listPullRequestLabels', input }).pipe(Effect.as(labels)),
				addPullRequestLabels: (input) =>
					Queue.offer(calls, { operation: 'addPullRequestLabels', input }).pipe(Effect.as(labels)),
				setPullRequestLabels: (input) =>
					Queue.offer(calls, { operation: 'setPullRequestLabels', input }).pipe(Effect.as(labels)),
				removePullRequestLabel: (input) =>
					Queue.offer(calls, { operation: 'removePullRequestLabel', input }).pipe(Effect.as(labels)),
				removeAllPullRequestLabels: (input) =>
					Queue.offer(calls, { operation: 'removeAllPullRequestLabels', input }).pipe(Effect.asVoid),
				fetchPullRequest: (input) =>
					Queue.offer(calls, { operation: 'fetchPullRequest', input }).pipe(Effect.as(pullRequestInfo)),
				listCheckRunsForRef: (input) =>
					Queue.offer(calls, { operation: 'listCheckRunsForRef', input }).pipe(Effect.as(checkRuns)),
				closePullRequest: (input) =>
					Queue.offer(calls, { operation: 'closePullRequest', input }).pipe(Effect.as(pullRequestInfo)),
				reopenPullRequest: (input) =>
					Queue.offer(calls, { operation: 'reopenPullRequest', input }).pipe(Effect.as(pullRequestInfo)),
				mergePullRequest: (input) =>
					Queue.offer(calls, { operation: 'mergePullRequest', input }).pipe(Effect.as(mergeResult)),
			})
			const pullRequest = GitHubPullRequest.make({ ref: pullRequestRef, mailboxKey: 'pull-request-mailbox' })
			const content = GitHubContent.make({ markdown: 'Please revise' })

			const results = yield* Effect.gen(function* () {
				return {
					reviewComment: yield* pullRequest.postReviewComment({
						content,
						commitId: 'head-sha',
						path: 'src/index.ts',
						location: { _tag: 'Range', startLine: 5, startSide: 'LEFT', line: 10, side: 'RIGHT' },
					}),
					files: yield* pullRequest.listFiles(),
					diff: yield* pullRequest.fetchDiff(),
					commits: yield* pullRequest.listCommits(),
					listedLabels: yield* pullRequest.listLabels(),
					addedLabels: yield* pullRequest.addLabels(['bug']),
					setLabels: yield* pullRequest.setLabels(['bug', 'urgent']),
					removedLabel: yield* pullRequest.removeLabel('needs review'),
					removedAllLabels: yield* pullRequest.removeAllLabels(),
					currentChecks: yield* pullRequest.listCheckRuns(),
					exactChecks: yield* pullRequest.listCheckRunsForRef('event-sha'),
					closed: yield* pullRequest.close(),
					reopened: yield* pullRequest.reopen(),
					merged: yield* pullRequest.merge({
						method: 'squash',
						expectedHeadSha: 'head-sha',
						commitTitle: 'Ship it',
						commitMessage: 'Complete implementation',
					}),
				}
			}).pipe(Effect.provide(layer))

			expect(results).toMatchObject({
				reviewComment,
				files,
				diff: 'complete diff',
				commits,
				listedLabels: labels,
				addedLabels: labels,
				setLabels: labels,
				removedLabel: labels,
				removedAllLabels: undefined,
				currentChecks: checkRuns,
				exactChecks: checkRuns,
				closed: pullRequestInfo,
				reopened: pullRequestInfo,
				merged: mergeResult,
			})
			expect(Array.from(yield* Queue.takeAll(calls))).toEqual([
				{
					operation: 'postPullRequestReviewComment',
					input: {
						pullRequest: pullRequestRef,
						content,
						commitId: 'head-sha',
						path: 'src/index.ts',
						location: { _tag: 'Range', startLine: 5, startSide: 'LEFT', line: 10, side: 'RIGHT' },
					},
				},
				{ operation: 'listPullRequestFiles', input: { pullRequest: pullRequestRef } },
				{ operation: 'fetchPullRequestDiff', input: { pullRequest: pullRequestRef } },
				{ operation: 'listPullRequestCommits', input: { pullRequest: pullRequestRef } },
				{ operation: 'listPullRequestLabels', input: { pullRequest: pullRequestRef } },
				{ operation: 'addPullRequestLabels', input: { pullRequest: pullRequestRef, labels: ['bug'] } },
				{
					operation: 'setPullRequestLabels',
					input: { pullRequest: pullRequestRef, labels: ['bug', 'urgent'] },
				},
				{
					operation: 'removePullRequestLabel',
					input: { pullRequest: pullRequestRef, label: 'needs review' },
				},
				{ operation: 'removeAllPullRequestLabels', input: { pullRequest: pullRequestRef } },
				{ operation: 'fetchPullRequest', input: { pullRequest: pullRequestRef } },
				{ operation: 'listCheckRunsForRef', input: { pullRequest: pullRequestRef, sha: 'head-sha' } },
				{ operation: 'listCheckRunsForRef', input: { pullRequest: pullRequestRef, sha: 'event-sha' } },
				{ operation: 'closePullRequest', input: { pullRequest: pullRequestRef } },
				{ operation: 'reopenPullRequest', input: { pullRequest: pullRequestRef } },
				{
					operation: 'mergePullRequest',
					input: {
						pullRequest: pullRequestRef,
						method: 'squash',
						expectedHeadSha: 'head-sha',
						commitTitle: 'Ship it',
						commitMessage: 'Complete implementation',
					},
				},
			])
		}),
	)

	it.effect('delegates check-run and Actions-job resource operations to GitHubApi', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<unknown>()
			const layer = Layer.mock(GitHubApi, {
				fetchCheckRun: (input) =>
					Queue.offer(calls, { operation: 'fetchCheckRun', input }).pipe(Effect.as(checkRunInfo)),
				listCheckRunAnnotations: (input) =>
					Queue.offer(calls, { operation: 'listCheckRunAnnotations', input }).pipe(Effect.as(annotations)),
				resolveActionsJob: (input) =>
					Queue.offer(calls, { operation: 'resolveActionsJob', input }).pipe(Effect.as(job)),
				fetchActionsJob: (input) =>
					Queue.offer(calls, { operation: 'fetchActionsJob', input }).pipe(Effect.as(jobInfo)),
				downloadActionsJobLog: (input) =>
					Queue.offer(calls, { operation: 'downloadActionsJobLog', input }).pipe(Effect.as('job log')),
			})
			const check = GitHubCheckRun.make({ ref: checkRunRef })
			const actionsJob = GitHubActionsJob.make({ ref: jobRef })

			const results = yield* Effect.gen(function* () {
				return {
					checkInfo: yield* check.fetchInfo(),
					annotations: yield* check.listAnnotations(),
					job: yield* check.resolveActionsJob(),
					jobInfo: yield* actionsJob.fetchInfo(),
					log: yield* actionsJob.downloadLog(),
				}
			}).pipe(Effect.provide(layer))

			expect(results).toEqual({ checkInfo: checkRunInfo, annotations, job, jobInfo, log: 'job log' })
			expect(Array.from(yield* Queue.takeAll(calls))).toEqual([
				{ operation: 'fetchCheckRun', input: { checkRun: checkRunRef } },
				{ operation: 'listCheckRunAnnotations', input: { checkRun: checkRunRef } },
				{ operation: 'resolveActionsJob', input: { checkRun: checkRunRef } },
				{ operation: 'fetchActionsJob', input: { job: jobRef } },
				{ operation: 'downloadActionsJobLog', input: { job: jobRef } },
			])
		}),
	)
})
