/**
 * This file turns a delivery's whole plan into Slack's plan stream chunks.
 *
 * Slack shows a plan as a stream message in plan mode. Its chunks change it step by step: a
 * `plan_update` sets the title, and a `task_update` creates or replaces one task, named by a stable ID.
 * There is no chunk that removes or moves a task, or clears a task's details or output.
 *
 * So a new plan is compared with the plan Slack last showed. When every task Slack shows is still there,
 * in the same order, with new tasks only at the end and no field cleared, the change is the chunks for
 * what differs. Anything else replaces the stream with a new one that shows the whole plan.
 */
import type { DeliveryPlan, DeliveryPlanItem } from '@humanlayer/channels-delivery-next'
import { Array as Arr, Data, Match, Predicate, Schema } from 'effect'

import { SlackMessageRef } from './SlackModels'
import { PlanUpdateChunk, TaskUpdateChunk, type SlackStreamChunk } from './SlackStreamChunk'

/** Where Slack shows a delivery's plan: its stream message. Saved as the plan's presentation; read only here. */
export const SlackPlanPresentation = Schema.TaggedStruct('SlackPlanStream', { message: SlackMessageRef })
export type SlackPlanPresentation = typeof SlackPlanPresentation.Type
export const SlackPlanPresentationJson = Schema.toCodecJson(SlackPlanPresentation)

/** Slack shows a title on every plan; a plan without one shows this. */
export const slackDefaultPlanTitle = 'Plan'

const planTitle = (plan: DeliveryPlan) => PlanUpdateChunk.make({ title: plan.title ?? slackDefaultPlanTitle })

/** One item as Slack's task: its state as Slack's status, with details while in progress and output after. */
export const slackPlanTask = (item: DeliveryPlanItem) => {
	const fields = Match.value(item.state).pipe(
		Match.tagsExhaustive({
			Pending: () => ({ status: 'pending' as const }),
			InProgress: ({ details }) =>
				Predicate.isUndefined(details) ? { status: 'in_progress' as const } : { status: 'in_progress' as const, details },
			Completed: ({ result }) =>
				Predicate.isUndefined(result) ? { status: 'complete' as const } : { status: 'complete' as const, output: result },
			Failed: ({ reason }) =>
				Predicate.isUndefined(reason) ? { status: 'error' as const } : { status: 'error' as const, output: reason },
		}),
	)
	return TaskUpdateChunk.make({ id: item.id, title: item.title, ...fields })
}

/** The chunks a new stream starts with: the title, then every task in order. */
export const slackPlanStartChunks = (plan: DeliveryPlan): ReadonlyArray<SlackStreamChunk> => [
	planTitle(plan),
	...plan.items.map(slackPlanTask),
]

/**
 * How to bring Slack from the plan it shows to a new one.
 *
 * - `Append`: send these chunks to the open stream; none when nothing Slack shows changed
 * - `Replace`: start a new stream with the whole plan
 */
export type SlackPlanChange = Data.TaggedEnum<{
	Append: { readonly chunks: ReadonlyArray<SlackStreamChunk> }
	Replace: {}
}>
export const SlackPlanChange = Data.taggedEnum<SlackPlanChange>()

const sameTask = Schema.toEquivalence(TaskUpdateChunk)

/** Whether going from one task to another needs a field cleared, which no chunk can do. */
const clearsField = (shown: TaskUpdateChunk, next: TaskUpdateChunk) =>
	(Predicate.isNotUndefined(shown.details) && Predicate.isUndefined(next.details)) ||
	(Predicate.isNotUndefined(shown.output) && Predicate.isUndefined(next.output))

/** Compare the plan Slack shows with the next one. */
export const slackPlanChange = (shown: DeliveryPlan, next: DeliveryPlan): SlackPlanChange => {
	const kept = next.items.slice(0, shown.items.length)
	const sameOrder =
		kept.length === shown.items.length && Arr.every(kept, (item, index) => item.id === shown.items[index]?.id)
	if (!sameOrder) return SlackPlanChange.Replace()
	const pairs = Arr.zip(shown.items.map(slackPlanTask), kept.map(slackPlanTask))
	if (pairs.some(([before, after]) => clearsField(before, after))) return SlackPlanChange.Replace()
	const changed = pairs.flatMap(([before, after]) => (sameTask(before, after) ? [] : [after]))
	const added = next.items.slice(shown.items.length).map(slackPlanTask)
	const title = planTitle(shown).title === planTitle(next).title ? [] : [planTitle(next)]
	return SlackPlanChange.Append({ chunks: [...title, ...changed, ...added] })
}
