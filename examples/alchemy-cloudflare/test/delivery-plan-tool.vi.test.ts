import { describe, it } from '@effect/vitest'
import {
	BatchId,
	DeliveryControl,
	DeliveryId,
	DeliveryMutationReceipt,
	DeliveryPlan,
	DeliveryPlanItem,
	DeliveryPlanItemId,
	DeliveryPlanItemState,
	PutDeliveryPlan,
	deliveryApiRoutes,
	makeDeliveryId,
	type DeliveryMutation,
} from '@humanlayer/channels-delivery'
import { GitHubId, GitHubIssue, GitHubIssueRef } from '@humanlayer/channels-github'
import { ToolResultFailure, ToolResultText } from '@humanlayer/fold-core'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Effect, Layer, Option, Redacted, Ref, Result } from 'effect'
import { HttpClient, HttpRouter, HttpServerRequest } from 'effect/http'

import { DeliveryApi } from '../src/DeliveryApi'
import { updateDeliveryPlan } from '../src/DeliveryPlanTool'
import { ActiveDelivery, ActiveDeliveryRecord, AgentSessionMessage } from '../src/DeliveryTurn'

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

const message = AgentSessionMessage.make({
	prompt: 'fix the bug',
	githubDiscussion: issue,
	deliveryId: makeDeliveryId({ mailboxKey: issue.mailboxKey, batchId: BatchId.make('batch-1') }),
	accessToken: Redacted.make('secret-token'),
})

interface Applied {
	readonly deliveryId: string
	readonly accessToken: string
	readonly mutation: DeliveryMutation
}

/**
 * `updateDeliveryPlan` over the real delivery API: the client sends HTTP requests to the delivery routes, which
 * apply them to a recording `DeliveryControl`. The active delivery is `saved`.
 */
const runUpdate = (plan: DeliveryPlan, saved: Option.Option<ActiveDeliveryRecord>) =>
	Effect.gen(function* () {
		const applied = yield* Ref.make<ReadonlyArray<Applied>>([])
		const deliveryControl = Layer.succeed(
			DeliveryControl,
			DeliveryControl.of({
				status: () => Effect.die('unexpected delivery status read'),
				apply: (input) =>
					Ref.update(applied, (all) => [
						...all,
						{
							deliveryId: input.deliveryId,
							accessToken: Redacted.value(input.accessToken),
							mutation: input.mutation,
						},
					]).pipe(
						Effect.as(
							DeliveryMutationReceipt.make({
								deliveryId: DeliveryId.make(input.deliveryId),
								status: 'accepted',
							}),
						),
					),
			}),
		)
		const deliveryRoutes = yield* HttpRouter.toHttpEffect(
			deliveryApiRoutes({ basePath: undefined }).pipe(Layer.provide(deliveryControl)),
		)
		const routesHttpClient = Cloudflare.toHttpClient({
			fetch: (request) =>
				deliveryRoutes.pipe(Effect.provideService(HttpServerRequest.HttpServerRequest, request), Effect.scoped),
		})
		const dependencies = Layer.mergeAll(
			DeliveryApi.layer.pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, routesHttpClient))),
			Layer.succeed(
				ActiveDelivery,
				ActiveDelivery.of({
					current: Effect.succeed(saved),
					save: () => Effect.die('unexpected save'),
					clear: () => Effect.die('unexpected clear'),
				}),
			),
		)

		const result = yield* updateDeliveryPlan(plan).pipe(Effect.result, Effect.provide(dependencies))
		return { result, applied: yield* Ref.get(applied) }
	})

const itemId = (id: string) => DeliveryPlanItemId.make(id)

const plan = DeliveryPlan.make({
	title: 'Fix the bug',
	items: [
		DeliveryPlanItem.make({
			id: itemId('find'),
			title: 'Find the cause',
			state: DeliveryPlanItemState.cases.Completed.make({ result: 'a missing await' }),
		}),
		DeliveryPlanItem.make({
			id: itemId('fix'),
			title: 'Fix it',
			state: DeliveryPlanItemState.cases.InProgress.make({}),
		}),
		DeliveryPlanItem.make({
			id: itemId('test'),
			title: 'Run the tests',
			state: DeliveryPlanItemState.cases.Pending.make({}),
		}),
	],
})

describe('update_plan', () => {
	it.effect('puts the whole plan on the active delivery', ({ expect }) =>
		Effect.gen(function* () {
			const { result, applied } = yield* runUpdate(plan, Option.some(ActiveDeliveryRecord.make({ message })))

			expect(result).toEqual(Result.succeed(ToolResultText.make({ text: 'Plan updated.' })))
			expect(applied).toEqual([
				{
					deliveryId: message.deliveryId,
					accessToken: 'secret-token',
					mutation: PutDeliveryPlan.make({ plan }),
				},
			])
		}),
	)

	it.effect('fails without the delivery ID or token when no delivery is active', ({ expect }) =>
		Effect.gen(function* () {
			const { result, applied } = yield* runUpdate(plan, Option.none())

			expect(result).toEqual(
				Result.fail(ToolResultFailure.make({ text: 'The plan could not be updated. Carry on with the work.' })),
			)
			expect(applied).toEqual([])
		}),
	)
})
