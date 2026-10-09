/**
 * What the agent is shown of its GitHub discussion before each request. The first request shows the issue or
 * pull request and everything said on it; later ones show only what is new since the agent's last turn. GitHub
 * IDs only grow, so "new" means an ID above the highest one the agent has been shown.
 */
import {
	GitHubId,
	type GitHubApi,
	type GitHubApiError,
	type GitHubIssue,
	type GitHubIssueComment,
	type GitHubParticipant,
	type GitHubPullRequest,
	type GitHubReview,
	type GitHubReviewComment,
} from '@humanlayer/channels-github'
import { Array as Arr, Effect, Match, Option, Predicate, Schema } from 'effect'

/** Longest comment body shown in full. */
const MAX_BODY_LENGTH = 4_000
/** Most text shown at once; the oldest comments are left out past this. */
const MAX_CONTEXT_LENGTH = 60_000

/** The comment that mentioned the bot; it is the request, so it is not repeated as context. */
export const MentionedIn = Schema.TaggedUnion({
	Comment: { id: GitHubId },
	ReviewComment: { id: GitHubId },
})
export type MentionedIn = typeof MentionedIn.Type

/** The highest comment, review, and line comment IDs the agent has been shown. */
export const DiscussionSeen = Schema.Struct({
	comment: Schema.Int,
	review: Schema.Int,
	reviewComment: Schema.Int,
})
export type DiscussionSeen = typeof DiscussionSeen.Type

/** What the agent is shown, and the IDs it has seen once it has been shown it. */
export interface DiscussionContext {
	readonly text: string
	readonly seen: DiscussionSeen
}

interface Entry {
	readonly id: number
	readonly text: string
}

const author = (participant: GitHubParticipant | null) =>
	participant === null ? 'a deleted user' : `@${participant.login}`

const clip = (body: string) =>
	body.length <= MAX_BODY_LENGTH
		? body
		: `${body.slice(0, MAX_BODY_LENGTH)}\n[Cut off at ${MAX_BODY_LENGTH} of ${body.length} characters.]`

const commentEntry = (comment: GitHubIssueComment): Entry => ({
	id: comment.ref.id,
	text: `${author(comment.author)} commented (comment ${comment.ref.id}):\n${clip(comment.body)}`,
})

const reviewEntry = (review: GitHubReview): Entry => ({
	id: review.ref.id,
	text: `${author(review.author)} reviewed (${review.state.replace('_', ' ')})${review.body ? `:\n${clip(review.body)}` : '.'}`,
})

const reviewCommentEntry = (comment: GitHubReviewComment): Entry => {
	const line = comment.line ?? comment.startLine
	const where = line === null || line === undefined ? comment.path : `${comment.path}:${line}`
	const thread = comment.inReplyToId ?? comment.ref.id
	return {
		id: comment.ref.id,
		text: `${author(comment.author)} commented on ${where} (line comment ${comment.ref.id}, thread ${thread}):\n${clip(comment.body)}`,
	}
}

const highest = (ids: ReadonlyArray<number>, floor: number) => Math.max(floor, ...ids)

/** The section's entries newer than `after`, oldest first. */
const newer = (entries: ReadonlyArray<Entry>, after: number) =>
	entries.filter((entry) => entry.id > after).toSorted((a, b) => a.id - b.id)

/** The newest entries whose text fits in `budget` characters, oldest first, and how many were left out. */
const newestThatFit = (entries: ReadonlyArray<Entry>, budget: number) => {
	const totals = Arr.scan(entries.toReversed(), 0, (used, entry) => used + entry.text.length).slice(1)
	const over = totals.findIndex((used) => used > budget)
	const count = over === -1 ? entries.length : over
	return { kept: entries.slice(entries.length - count), omitted: entries.length - count }
}

/**
 * The heading, then each section that has entries. A section past its share of {@link MAX_CONTEXT_LENGTH}
 * keeps its newest entries, with a note of how many older ones were left out.
 */
const render = (heading: string, sections: ReadonlyArray<readonly [string, ReadonlyArray<Entry>]>) => {
	const filled = sections.filter(([, entries]) => entries.length > 0)
	const budget = MAX_CONTEXT_LENGTH / Math.max(filled.length, 1)
	return [
		heading,
		...filled.map(([title, entries]) => {
			const { kept, omitted } = newestThatFit(entries, budget)
			const note =
				omitted === 0 ? [] : [`[${omitted} older entries left out. Use the GitHub tools to read them.]`]
			return [title, ...note, ...kept.map((entry) => entry.text)].join('\n\n')
		}),
	].join('\n\n')
}

/**
 * Read the discussion and show the agent what it has not seen: everything when `seen` is empty, otherwise
 * what is new. The bot's own comments and the comment that mentioned it are left out of the text but count as
 * seen. The text is empty when nothing is new.
 */
export const readDiscussionContext = Effect.fn('agent_session.read_discussion')(function* (input: {
	readonly discussion: GitHubIssue | GitHubPullRequest
	readonly seen: Option.Option<DiscussionSeen>
	readonly botUserId: number
	readonly mentionedIn: MentionedIn | undefined
}) {
	const { discussion, botUserId, mentionedIn } = input
	const seen = Option.getOrElse(input.seen, (): DiscussionSeen => ({
		comment: 0,
		review: 0,
		reviewComment: 0,
	}))
	const first = Option.isNone(input.seen)
	const notBot = (participant: GitHubParticipant | null) => participant?.id !== botUserId
	const isMention = (tag: MentionedIn['_tag'], id: number) =>
		Predicate.isTagged(mentionedIn, tag) && mentionedIn.id === id

	const allComments = yield* discussion.listComments()
	const comments = allComments.filter((comment) => notBot(comment.author) && !isMention('Comment', comment.ref.id))
	const pullRequestActivity = yield* Match.value(discussion).pipe(
		Match.tagsExhaustive({
			GitHubIssue: () => Effect.succeed({ reviews: [], reviewComments: [] }),
			GitHubPullRequest: (pullRequest) =>
				Effect.all({
					reviews: pullRequest.listReviews(),
					reviewComments: pullRequest.listReviewComments(),
				}),
		}),
	)
	const reviews = pullRequestActivity.reviews.filter((review) => notBot(review.author))
	const reviewComments = pullRequestActivity.reviewComments.filter(
		(comment) => notBot(comment.author) && !isMention('ReviewComment', comment.ref.id),
	)

	const sections = [
		['Comments:', newer(comments.map(commentEntry), seen.comment)],
		['Reviews:', newer(reviews.map(reviewEntry), seen.review)],
		['Line comments:', newer(reviewComments.map(reviewCommentEntry), seen.reviewComment)],
	] as const
	const nextSeen: DiscussionSeen = {
		comment: highest(
			allComments.map((comment) => comment.ref.id),
			seen.comment,
		),
		review: highest(
			pullRequestActivity.reviews.map((review) => review.ref.id),
			seen.review,
		),
		reviewComment: highest(
			pullRequestActivity.reviewComments.map((comment) => comment.ref.id),
			seen.reviewComment,
		),
	}

	if (first) {
		const { kind, info } = yield* Match.value(discussion).pipe(
			Match.tagsExhaustive({
				GitHubIssue: (issue) => Effect.map(issue.fetchInfo(), (info) => ({ kind: 'Issue', info })),
				GitHubPullRequest: (pullRequest) =>
					Effect.map(pullRequest.fetchInfo(), (info) => ({ kind: 'Pull request', info })),
			}),
		)
		const heading = `${kind} #${info.ref.number} "${info.title}" (${info.state}), opened by ${author(info.author)}:\n${clip(info.body ?? '(no description)')}`
		return {
			text: `<github-discussion>\n${render(heading, sections)}\n</github-discussion>\n\n`,
			seen: nextSeen,
		} satisfies DiscussionContext
	}

	if (sections.every(([, entries]) => entries.length === 0)) return { text: '', seen: nextSeen }
	return {
		text: `<github-discussion>\n${render('New since your last turn:', sections)}\n</github-discussion>\n\n`,
		seen: nextSeen,
	} satisfies DiscussionContext
}) satisfies (...args: ReadonlyArray<never>) => Effect.Effect<DiscussionContext, GitHubApiError, GitHubApi>
