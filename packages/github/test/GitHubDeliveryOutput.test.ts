import { assert, it } from '@effect/vitest'
import { finalMessageOperationId, FinalMessageOperation, PendingDeliveryOperation } from '@humanlayer/channels-delivery'
import { Effect, Layer, Queue, Schema } from 'effect'

import { GitHub, GitHubComment } from '../src/GitHub'
import { deliverGitHubFinalMessage } from '../src/GitHubDeliveryOutput'
import { GitHubDiscussionRef } from '../src/GitHubResource'

it.effect('decodes a saved GitHub destination and creates one final discussion comment', () =>
	Effect.gen(function* () {
		const comments = yield* Queue.unbounded<{ readonly body: string }>()
		const issue = GitHubDiscussionRef.make({
			kind: 'github.issue',
			repository: { kind: 'github.repository', installationId: 7, id: 11, owner: 'acme', name: 'repo' },
			number: 13,
		})
		const github = Layer.mock(GitHub, {
			createComment: (input) =>
				Queue.offer(comments, { body: input.body }).pipe(
					Effect.as(
						GitHubComment.make({
							ref: { kind: 'github.issue-comment', issue, id: 17 },
							data: {
								id: 17,
								body: input.body,
								html_url: 'https://github.test/acme/repo/issues/13#issuecomment-17',
								user: { id: 19, login: 'bot', type: 'Bot' },
							},
						}),
					),
				),
		})
		const deliveryId = 'delivery:v2:github-final'
		const destination = yield* Schema.encodeEffect(Schema.fromJsonString(GitHubDiscussionRef))(issue)
		const receipt = yield* deliverGitHubFinalMessage(
			FinalMessageOperation.make({
				operationId: finalMessageOperationId(deliveryId),
				deliveryId,
				outcome: 'failed',
				markdown: 'Final **GitHub** answer',
				provider: 'github',
				installation: '7',
				destination,
				presentation: 'github.activity',
				presentationVersion: '1',
				state: PendingDeliveryOperation.make({ attempt: 0, readyAt: 0, hadAmbiguousAttempt: false }),
			}),
		).pipe(Effect.provide(github))
		assert.deepStrictEqual(yield* Queue.take(comments), { body: 'Final **GitHub** answer' })
		assert.match(receipt.providerReceipt, /17/)
	}),
)
