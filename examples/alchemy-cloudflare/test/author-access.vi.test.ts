import { describe, it } from '@effect/vitest'
import {
	type GitHubAccessLevel,
	GitHubApi,
	GitHubApiError,
	GitHubDiscussionRef,
	GitHubEventId,
	GitHubId,
	GitHubIssue,
	GitHubIssueComment,
	GitHubIssueCommentCreated,
	GitHubIssueRef,
	GitHubParticipant,
} from '@humanlayer/channels-github'
import {
	SlackChannelId,
	SlackMarkdownContent,
	SlackMessage,
	SlackMessageRef,
	SlackMessageTs,
	SlackParticipant,
	SlackTeamId,
	SlackThreadRef,
	SlackUserId,
} from '@humanlayer/channels-slack'
import { Effect, Layer, Queue } from 'effect'

import { eventsFromGitHubWriters, isFromInstallationWorkspace, isFromSlackWorkspace } from '../src/AuthorAccess'

const issue = GitHubIssue.make({
	ref: GitHubIssueRef.make({
		installationId: GitHubId.make(100),
		repositoryId: GitHubId.make(200),
		owner: 'humanlayer',
		repository: 'effect-channels',
		number: GitHubId.make(42),
	}),
	mailboxKey: 'github:issue:42',
})

const person = (id: number, login: string) => GitHubParticipant.make({ id: GitHubId.make(id), login, type: 'User' })
const grants = (level: GitHubAccessLevel): Effect.Effect<GitHubAccessLevel, GitHubApiError> => Effect.succeed(level)
const comment = (author: GitHubParticipant) =>
	GitHubIssueCommentCreated.make({
		eventId: GitHubEventId.make(`comment-by-${author.login}`),
		issue,
		actor: author,
		comment: GitHubIssueComment.make({
			ref: { discussion: GitHubDiscussionRef.cases.Issue.make({ ref: issue.ref }), id: author.id },
			body: 'hello',
			url: 'https://github.com/humanlayer/effect-channels/issues/42',
			author,
		}),
	})

/** A GitHub API that answers only `fetchUserAccess`, from `access` by login, and records each lookup. */
const gitHubApi = (
	lookups: Queue.Queue<string>,
	access: ReadonlyMap<string, Effect.Effect<GitHubAccessLevel, GitHubApiError>>,
) =>
	Layer.mock(GitHubApi, {
		fetchUserAccess: ({ repository, login }) =>
			Effect.gen(function* () {
				if (repository.repositoryId !== issue.ref.repositoryId) {
					return yield* Effect.die(new Error(`Unexpected repository ${repository.repositoryId}`))
				}
				yield* Queue.offer(lookups, login)
				const answer = access.get(login)
				if (answer === undefined) return yield* Effect.die(new Error(`Unexpected lookup for ${login}`))
				return yield* answer
			}),
	})

describe('GitHub author access', () => {
	it.effect('keeps events from write access or higher, looking up each author once', ({ expect }) =>
		Effect.gen(function* () {
			const lookups = yield* Queue.unbounded<string>()
			const owner = person(1, 'K-Mistele')
			const outsider = person(2, 'octocat')
			const maintainer = person(3, 'maintainer')
			const triager = person(4, 'triager')
			const events = [comment(owner), comment(outsider), comment(owner), comment(maintainer), comment(triager)]

			const allowed = yield* eventsFromGitHubWriters({ discussion: issue, events }).pipe(
				Effect.provide(
					gitHubApi(
						lookups,
						new Map([
							['K-Mistele', grants('admin')],
							['octocat', grants('read')],
							['maintainer', grants('maintain')],
							['triager', grants('triage')],
						]),
					),
				),
			)

			expect(allowed.map((event) => event.actor.login)).toEqual(['K-Mistele', 'K-Mistele', 'maintainer'])
			expect(Array.from(yield* Queue.takeAll(lookups))).toEqual(['K-Mistele', 'octocat', 'maintainer', 'triager'])
		}),
	)

	it.effect('counts an author GitHub does not know as a user as no access, and drops their event', ({ expect }) =>
		Effect.gen(function* () {
			const lookups = yield* Queue.unbounded<string>()
			const notAUser = GitHubApiError.make({
				operation: 'fetch_user_access',
				reason: 'not_found',
				retryable: false,
				status: 404,
			})

			const allowed = yield* eventsFromGitHubWriters({
				discussion: issue,
				events: [comment(person(6, 'dependabot[bot]')), comment(person(1, 'K-Mistele'))],
			}).pipe(
				Effect.provide(
					gitHubApi(
						lookups,
						new Map([
							['dependabot[bot]', Effect.fail(notAUser)],
							['K-Mistele', grants('admin')],
						]),
					),
				),
			)

			expect(allowed.map((event) => event.actor.login)).toEqual(['K-Mistele'])
			expect(Array.from(yield* Queue.takeAll(lookups))).toEqual(['dependabot[bot]', 'K-Mistele'])
		}),
	)

	it.effect('fails when a lookup fails, rather than letting the event through', ({ expect }) =>
		Effect.gen(function* () {
			const lookups = yield* Queue.unbounded<string>()
			const unavailable = GitHubApiError.make({
				operation: 'fetch_user_access',
				reason: 'unavailable',
				retryable: true,
			})

			const error = yield* eventsFromGitHubWriters({
				discussion: issue,
				events: [comment(person(1, 'K-Mistele')), comment(person(5, 'ghost'))],
			}).pipe(
				Effect.provide(
					gitHubApi(
						lookups,
						new Map([
							['K-Mistele', grants('admin')],
							['ghost', Effect.fail(unavailable)],
						]),
					),
				),
				Effect.flip,
			)

			expect(error).toEqual(unavailable)
			expect(Array.from(yield* Queue.takeAll(lookups))).toEqual(['K-Mistele', 'ghost'])
		}),
	)
})

describe('Slack author workspace', () => {
	const teamId = SlackTeamId.make('T_HOME')
	const channelId = SlackChannelId.make('C_CONNECT')
	const messageTs = SlackMessageTs.make('1700000000.000001')
	const message = (authorTeamId?: string) => {
		const fields = {
			ref: SlackMessageRef.make({ teamId, channelId, messageTs }),
			thread: SlackThreadRef.make({ teamId, channelId, threadTs: messageTs, isDm: false }),
			author: SlackParticipant.make({
				userId: SlackUserId.make('U_AUTHOR'),
				userName: 'author',
				fullName: 'Author',
				isBot: false,
				isMe: false,
			}),
			content: SlackMarkdownContent.make({ markdown: '<@U_BOT> handoff' }),
			files: [],
			metadata: {},
		}
		return authorTeamId === undefined
			? SlackMessage.make(fields)
			: SlackMessage.make({ ...fields, authorTeamId: SlackTeamId.make(authorTeamId) })
	}

	it.effect('treats an absent or matching author workspace as local, and another one as foreign', ({ expect }) =>
		Effect.gen(function* () {
			expect(isFromInstallationWorkspace(message())).toBe(true)
			expect(isFromInstallationWorkspace(message('T_HOME'))).toBe(true)
			expect(isFromInstallationWorkspace(message('T_GUEST'))).toBe(false)
			expect(yield* isFromSlackWorkspace(message())).toBe(true)
			expect(yield* isFromSlackWorkspace(message('T_GUEST'))).toBe(false)
		}),
	)
})
