/**
 * This file turns a delivery's plan into a Slack plan block.
 *
 * Slack shows the plan as one message holding a plan block: a title, then one task per item with its
 * status. The first plan posts the message; each later plan replaces the whole block with `chat.update`,
 * so the message stays where it is and any change, such as a removed task, is shown as it is.
 */
import type { DeliveryPlan, DeliveryPlanItem } from '@humanlayer/channels-delivery'
import { Match, Predicate, Schema } from 'effect'

import { SlackMessageRef, SlackPlan, SlackPlanTask } from './SlackModels'

/** Where Slack shows a delivery's plan: its plan message. Saved as the plan's presentation; read only here. */
export const SlackPlanPresentation = Schema.TaggedStruct('SlackPlanMessage', { message: SlackMessageRef })
export type SlackPlanPresentation = typeof SlackPlanPresentation.Type
export const SlackPlanPresentationJson = Schema.toCodecJson(SlackPlanPresentation)

/** Slack shows a title on every plan; a plan without one shows this. */
export const slackDefaultPlanTitle = 'Plan'

/** One item as Slack's task: its state as Slack's status, with details while in progress and output after. */
export const slackPlanTask = (item: DeliveryPlanItem) => {
	const task = { id: item.id, title: item.title }
	return Match.value(item.state).pipe(
		Match.tagsExhaustive({
			Pending: () => SlackPlanTask.make({ ...task, status: 'pending' }),
			InProgress: ({ details }) =>
				SlackPlanTask.make(
					Predicate.isUndefined(details) ? { ...task, status: 'in_progress' } : { ...task, status: 'in_progress', details },
				),
			Completed: ({ result }) =>
				SlackPlanTask.make(
					Predicate.isUndefined(result) ? { ...task, status: 'complete' } : { ...task, status: 'complete', output: result },
				),
			Failed: ({ reason }) =>
				SlackPlanTask.make(
					Predicate.isUndefined(reason) ? { ...task, status: 'error' } : { ...task, status: 'error', output: reason },
				),
		}),
	)
}

/** The plan as Slack's plan block. */
export const slackPlan = (plan: DeliveryPlan) =>
	SlackPlan.make({ title: plan.title ?? slackDefaultPlanTitle, tasks: plan.items.map(slackPlanTask) })
