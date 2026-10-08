/**
 * The agent's `update_plan` tool: its checklist for the delivery it is working on, which GitHub shows as one
 * comment it edits as the plan changes. The model sends a `DeliveryPlan`, the delivery API's own schema. The
 * tool finds the delivery in the AgentSession's saved state, so the model never sees the delivery's ID or
 * token.
 */
import { DeliveryPlan } from '@humanlayer/channels-delivery'
import { defineTool, ToolResultFailure, ToolResultText, type FoldTool } from '@humanlayer/fold-core'
import { Data, Effect, Option } from 'effect'

import { DeliveryApi } from './DeliveryApi'
import { ActiveDelivery, targetOf } from './DeliveryTurn'

/** No delivery is being worked on, so there is nowhere to show the plan. */
export class NoActiveDelivery extends Data.TaggedError('NoActiveDelivery')<{}> {}

/** Replace the active delivery's plan. */
export const putActiveDeliveryPlan = Effect.fn('agent_session.put_delivery_plan')(function* (plan: DeliveryPlan) {
	const active = yield* ActiveDelivery
	const api = yield* DeliveryApi
	const record = yield* active.current
	if (Option.isNone(record)) return yield* new NoActiveDelivery()
	yield* api.plan.put({ ...targetOf(record.value.message), plan })
	yield* Effect.logInfo('agent_session.plan_updated').pipe(
		Effect.annotateLogs({
			'delivery.id': record.value.message.deliveryId,
			items: plan.items.map((item) => `${item.id}:${item.state._tag}`),
		}),
	)
})

/** The tool's work: show the plan on the active delivery. */
export const updateDeliveryPlan = (plan: DeliveryPlan) =>
	putActiveDeliveryPlan(plan).pipe(
		Effect.tapError((error) => Effect.logWarning('agent_session.update_plan failed', error)),
		Effect.mapError(() =>
			ToolResultFailure.make({ text: 'The plan could not be updated. Carry on with the work.' }),
		),
		Effect.as(ToolResultText.make({ text: 'Plan updated.' })),
	)

export const updatePlanTool: FoldTool<ActiveDelivery | DeliveryApi> = defineTool({
	name: 'update_plan',
	description:
		'Show your plan for this request as a checklist on the GitHub issue or pull request. Send the whole ' +
		"plan every time: an optional title, and every step in order, each keeping its ID across updates. A step's " +
		'state is {"_tag": "Pending"}, {"_tag": "InProgress", "details"?}, {"_tag": "Completed", "result"?}, or ' +
		'{"_tag": "Failed", "reason"?}. Keep exactly one step InProgress while you work, and update the plan as ' +
		'steps complete or fail.',
	parameters: DeliveryPlan,
	success: ToolResultText,
	failure: ToolResultFailure,
	handler: updateDeliveryPlan,
})
