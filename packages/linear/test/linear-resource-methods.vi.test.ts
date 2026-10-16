import { describe, it } from '@effect/vitest'
import { Effect, Layer, Queue } from 'effect'

import { LinearApi } from '../src/LinearApi'
import {
	LinearAttachmentId,
	LinearCommentId,
	LinearIssueLabelId,
	LinearReactionId,
	LinearUserId,
	LinearWorkflowStateId,
} from '../src/LinearIdentity'
import { LinearCommentRef, LinearContent } from '../src/LinearModels'
import { LinearCommentRecord, LinearIssueAttachmentRecord, LinearReactionRecord } from '../src/LinearResourceRecords'
import { LinearComment, LinearIssue, LinearIssueAttachment, LinearReaction } from '../src/LinearResources'
import { issue } from './api-test-fixtures'

describe('Linear resource methods', () => {
	it.effect('delegates every API-backed resource method with provider-native requests', ({ expect }) =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<{ method: string; input: unknown }>()
			const info = {
				ref: issue,
				identifier: 'CORE-1',
				title: 'Issue',
				description: null,
				priority: 1,
				url: '',
				state: { id: LinearWorkflowStateId.make('state'), name: 'Todo', type: 'unstarted' },
				labels: [],
				assignee: null,
				delegate: null,
			}
			const commentRef = LinearCommentRef.make({ ...issue, commentId: LinearCommentId.make('comment-1') })
			const commentRecord = LinearCommentRecord.make({
				ref: commentRef,
				issue,
				parentCommentId: null,
				content: LinearContent.make({ markdown: 'body' }),
				author: null,
				files: [],
			})
			const reactionRecord = LinearReactionRecord.make({
				id: LinearReactionId.make('reaction-1'),
				issueId: issue.issueId,
				commentId: null,
				emoji: 'eyes',
				author: null,
				ref: { issue, reactionId: LinearReactionId.make('reaction-1') },
			})
			const attachmentRecord = LinearIssueAttachmentRecord.make({
				id: LinearAttachmentId.make('attachment-1'),
				issueId: issue.issueId,
				title: 'Card',
				subtitle: null,
				url: 'https://example.com',
				ref: { issue, attachmentId: LinearAttachmentId.make('attachment-1') },
			})
			const comment = LinearComment.fromRecord(commentRecord)
			const reaction = LinearReaction.make(reactionRecord)
			const attachment = LinearIssueAttachment.make(attachmentRecord)
			const record =
				<Input, A>(method: string, output: A) =>
				(input: Input) =>
					Queue.offer(calls, { method, input }).pipe(Effect.as(output))
			const layer = Layer.mock(LinearApi, {
				getIssue: record('getIssue', info),
				updateIssue: record('updateIssue', info),
				listAssignableUsers: record('listAssignableUsers', {
					users: [],
					pageInfo: { endCursor: null, hasNextPage: false },
				}),
				listAppUsers: record('listAppUsers', { users: [], pageInfo: { endCursor: null, hasNextPage: false } }),
				getUser: record('getUser', {
					id: LinearUserId.make('user-1'),
					name: 'User',
					email: null,
					active: true,
					app: false,
					isAssignable: true,
					canAccessAnyPublicTeam: false,
					teamIds: [],
				}),
				listIssueComments: record('listIssueComments', [commentRecord]),
				listIssueAttachments: record('listIssueAttachments', [attachmentRecord]),
				createComment: record('createComment', commentRecord),
				updateComment: record('updateComment', commentRecord),
				deleteComment: record('deleteComment', undefined),
				createReaction: record('createReaction', reactionRecord),
				deleteReaction: record('deleteReaction', undefined),
				createAttachment: record('createAttachment', attachmentRecord),
				updateAttachment: record('updateAttachment', attachmentRecord),
				deleteAttachment: record('deleteAttachment', undefined),
			})
			const resource = LinearIssue.make({
				ref: issue,
				mailboxKey: 'mailbox',
				identifier: 'CORE-1',
				number: 1,
				title: 'Issue',
				description: null,
				priority: 1,
				url: '',
				team: null,
				creator: null,
				files: [],
			})
			const run = <A, E>(effect: Effect.Effect<A, E, LinearApi>) => effect.pipe(Effect.provide(layer))
			yield* run(resource.fetchInfo())
			yield* run(resource.listAssignableUsers({ first: 10, query: 'u' }))
			yield* run(resource.listAppUsers({ after: 'cursor' }))
			yield* run(resource.getUser(LinearUserId.make('user-1')))
			const listed = yield* run(resource.listComments())
			expect(listed[0]).toBeInstanceOf(LinearComment)
			const attachments = yield* run(resource.listAttachments())
			expect(attachments[0]).toBeInstanceOf(LinearIssueAttachment)
			expect(yield* run(resource.postComment({ markdown: 'new' }))).toBeInstanceOf(LinearComment)
			yield* run(resource.setStatus(LinearWorkflowStateId.make('done')))
			yield* run(resource.setPriority(2))
			yield* run(resource.addLabels([LinearIssueLabelId.make('label')]))
			yield* run(resource.removeLabels([LinearIssueLabelId.make('label')]))
			yield* run(resource.assignTo(LinearUserId.make('human')))
			yield* run(resource.delegateTo(LinearUserId.make('app')))
			expect(yield* run(resource.addReaction('eyes'))).toBeInstanceOf(LinearReaction)
			expect(yield* run(resource.createAttachment({ url: 'https://example.com', title: 'Card' }))).toBeInstanceOf(
				LinearIssueAttachment,
			)
			expect(yield* run(comment.reply({ markdown: 'reply' }))).toBeInstanceOf(LinearComment)
			expect(yield* run(comment.update({ markdown: 'edit' }))).toBeInstanceOf(LinearComment)
			yield* run(comment.remove())
			yield* run(comment.addReaction('thumbsup'))
			yield* run(reaction.remove())
			expect(yield* run(attachment.update({ title: 'Renamed', subtitle: null }))).toBeInstanceOf(
				LinearIssueAttachment,
			)
			yield* run(attachment.remove())

			const recorded = Array.from(yield* Queue.takeAll(calls))
			expect(recorded.map(({ method }) => method)).toEqual([
				'getIssue',
				'listAssignableUsers',
				'listAppUsers',
				'getUser',
				'listIssueComments',
				'listIssueAttachments',
				'createComment',
				'updateIssue',
				'updateIssue',
				'updateIssue',
				'updateIssue',
				'updateIssue',
				'updateIssue',
				'createReaction',
				'createAttachment',
				'createComment',
				'updateComment',
				'deleteComment',
				'createReaction',
				'deleteReaction',
				'updateAttachment',
				'deleteAttachment',
			])
			expect(recorded.find(({ method }) => method === 'updateAttachment')?.input).toEqual({
				attachment: attachment.ref,
				input: { title: 'Renamed', subtitle: null },
			})
			expect(recorded.filter(({ method }) => method === 'updateIssue').map(({ input }) => input)).toEqual([
				{ issue, update: { stateId: 'done' } },
				{ issue, update: { priority: 2 } },
				{ issue, update: { addLabelIds: ['label'] } },
				{ issue, update: { removeLabelIds: ['label'] } },
				{ issue, update: { assigneeId: 'human' } },
				{ issue, update: { delegateId: 'app' } },
			])
		}),
	)
})
