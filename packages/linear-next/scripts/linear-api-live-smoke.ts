import { Config, Effect, Redacted, Ref } from 'effect'

import {
	LinearApi,
	LinearApiLiveOptions,
	LinearAuth,
	LinearIssueId,
	LinearIssueLabelId,
	LinearIssueRef,
	LinearOrganizationId,
	LinearTeamId,
	LinearUserId,
	LinearWorkflowStateId,
	makeLinearApiLive,
} from '../src/index'

const required = (name: string) => {
	const value = Bun.env[name]
	if (!value) throw new Error(`Missing ${name}`)
	return value
}

const mutationInputs = () => {
	const marker = Bun.env.LINEAR_SMOKE_CONFIRM_MUTATIONS
	if (!marker) return null
	if (!/^channels-live-p4-\d+$/.test(marker))
		throw new Error('LINEAR_SMOKE_CONFIRM_MUTATIONS must end in a numeric timestamp: channels-live-p4-<timestamp>')
	const priority = Number(required('LINEAR_SMOKE_PRIORITY'))
	if (!Number.isInteger(priority) || priority < 0 || priority > 4)
		throw new Error('LINEAR_SMOKE_PRIORITY must be a finite integer from 0 through 4')
	return {
		marker,
		priority,
		stateId: LinearWorkflowStateId.make(required('LINEAR_SMOKE_STATE_ID')),
		labelId: LinearIssueLabelId.make(required('LINEAR_SMOKE_LABEL_ID')),
		assigneeId: LinearUserId.make(required('LINEAR_SMOKE_ASSIGNEE_ID')),
		delegateId: LinearUserId.make(required('LINEAR_SMOKE_DELEGATE_ID')),
	}
}

const program = Effect.gen(function* () {
	const api = yield* LinearApi
	const issue = yield* api.getIssue({
		issue: LinearIssueRef.make({
			organizationId: LinearOrganizationId.make(required('LINEAR_ORGANIZATION_ID')),
			teamId: LinearTeamId.make('resolved-by-get-issue'),
			issueId: LinearIssueId.make(required('LINEAR_SMOKE_ISSUE_ID')),
		}),
	})
	const [assignable, apps, comments, attachments] = yield* Effect.all([
		api.listAssignableUsers({ issue: issue.ref }),
		api.listAppUsers({ issue: issue.ref }),
		api.listIssueComments({ issue: issue.ref }),
		api.listIssueAttachments({ issue: issue.ref }),
	])
	const knownUserId = LinearUserId.make(required('LINEAR_SMOKE_USER_ID'))
	const knownUser = yield* api.getUser({ issue: issue.ref, userId: knownUserId })
	console.info(
		JSON.stringify({
			issue: issue.identifier,
			assignable: assignable.users.length,
			apps: apps.users.length,
			comments: comments.length,
			attachments: attachments.length,
			knownUser: knownUser.id,
		}),
	)

	const inputs = mutationInputs()
	if (inputs === null) return
	const comments = yield* Ref.make<ReadonlyArray<Parameters<typeof api.deleteComment>[0]['comment']>>([])
	const attachmentsToDelete = yield* Ref.make<
		ReadonlyArray<Parameters<typeof api.deleteAttachment>[0]['attachment']>
	>([])
	const cleanup = Effect.gen(function* () {
		for (const attachment of yield* Ref.get(attachmentsToDelete))
			yield* api.deleteAttachment({ attachment }).pipe(Effect.ignore)
		for (const comment of [...(yield* Ref.get(comments))].reverse())
			yield* api.deleteComment({ comment }).pipe(Effect.ignore)
	})
	yield* Effect.gen(function* () {
		yield* api.updateIssue({ issue: issue.ref, update: { stateId: inputs.stateId } })
		yield* api.updateIssue({ issue: issue.ref, update: { priority: inputs.priority } })
		yield* api.updateIssue({ issue: issue.ref, update: { addLabelIds: [inputs.labelId] } })
		yield* api.updateIssue({ issue: issue.ref, update: { removeLabelIds: [inputs.labelId] } })
		yield* api.updateIssue({ issue: issue.ref, update: { assigneeId: inputs.assigneeId } })
		yield* api.updateIssue({ issue: issue.ref, update: { delegateId: inputs.delegateId } })
		const comment = yield* api.createComment({ issue: issue.ref, content: { markdown: inputs.marker } })
		yield* Ref.update(comments, (refs) => [...refs, comment.ref])
		const edited = yield* api.updateComment({
			comment: comment.ref,
			content: { markdown: `${inputs.marker} edited` },
		})
		const reply = yield* api.createComment({
			issue: issue.ref,
			parentId: edited.ref.commentId,
			content: { markdown: `${inputs.marker} reply` },
		})
		yield* Ref.update(comments, (refs) => [...refs, reply.ref])
		const reaction = yield* api.createReaction({ target: { _tag: 'Comment', comment: reply.ref }, emoji: 'eyes' })
		yield* api.deleteReaction({ issue: issue.ref, reactionId: reaction.ref.reactionId })
		const card = yield* api.createAttachment({
			issue: issue.ref,
			input: { url: 'https://example.com', title: inputs.marker },
		})
		yield* Ref.update(attachmentsToDelete, (refs) => [...refs, card.ref])
		yield* api.updateAttachment({ attachment: card.ref, input: { title: `${inputs.marker} updated` } })
		console.info(JSON.stringify({ mutationMarker: inputs.marker, completed: true }))
	}).pipe(Effect.ensuring(cleanup))
})

const layer = makeLinearApiLive(
	LinearApiLiveOptions.make({
		auth: LinearAuth.clientCredentials({
			clientId: Config.succeed(required('LINEAR_CLIENT_ID')),
			clientSecret: Config.succeed(Redacted.make(required('LINEAR_CLIENT_SECRET'))),
		}),
		organizationId: Config.succeed(LinearOrganizationId.make(required('LINEAR_ORGANIZATION_ID'))),
		appUserId: Config.succeed(LinearUserId.make(required('LINEAR_APP_USER_ID'))),
	}),
)

await Effect.runPromise(program.pipe(Effect.provide(layer)))
