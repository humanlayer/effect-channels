/**
 * This file defines a delivery's plan: the task list a remote worker keeps up to date while it works,
 * such as "inspect logs, rotate secret, verify deployment", each with its state.
 *
 * The remote worker always sends the whole plan, never a change to one item. The store keeps the latest
 * as the desired plan, and each new one advances its revision. Rapid changes coalesce: a new plan
 * replaces a `RenderPlan` still waiting to be sent, so only the latest is sent; one already being sent
 * is followed by a new operation.
 *
 * The store also keeps the plan a provider last showed, exactly as it was sent, with the provider's own
 * reference to where it shows it. A provider that can only change a plan step by step, such as Slack's
 * plan stream, compares the two. The stored plan, not any provider's rendering of it, is authoritative.
 *
 * ```text
 * PlanState
 * ├── desired      revision and the whole latest plan
 * └── rendered     revision, the exact plan last shown, and the provider's presentation
 * ```
 */
import { Array as Arr, Match, Predicate, Schema } from 'effect'

/** The most items one plan holds. Slack's plan shows at most 50 tasks. */
export const DELIVERY_PLAN_MAX_ITEMS = 50

/** Names one plan item. The remote worker keeps it stable across revisions. */
export const DeliveryPlanItemId = Schema.NonEmptyString.check(
	Schema.isMaxLength(64),
	Schema.isPattern(/^[A-Za-z0-9._-]+$/),
).pipe(Schema.brand('DeliveryPlanItemId'))
export type DeliveryPlanItemId = typeof DeliveryPlanItemId.Type

/** A plan's or an item's title. Slack shows at most 256 characters. */
export const DeliveryPlanTitle = Schema.NonEmptyString.check(Schema.isMaxLength(256))

/** A short note on an item: what it is doing, what it found, or why it failed. */
export const DeliveryPlanNote = Schema.NonEmptyString.check(Schema.isMaxLength(2_000))

/** Where one item is. */
export const DeliveryPlanItemState = Schema.TaggedUnion({
	Pending: {},
	InProgress: { details: Schema.optionalKey(DeliveryPlanNote) },
	Completed: { result: Schema.optionalKey(DeliveryPlanNote) },
	Failed: { reason: Schema.optionalKey(DeliveryPlanNote) },
})
export type DeliveryPlanItemState = typeof DeliveryPlanItemState.Type

export const DeliveryPlanItem = Schema.Struct({
	id: DeliveryPlanItemId,
	title: DeliveryPlanTitle,
	state: DeliveryPlanItemState,
})
export type DeliveryPlanItem = typeof DeliveryPlanItem.Type

/** The whole plan, in the order it is shown. Item IDs are unique. */
export const DeliveryPlan = Schema.Struct({
	title: Schema.optionalKey(DeliveryPlanTitle),
	items: Schema.Array(DeliveryPlanItem).check(
		Schema.isMaxLength(DELIVERY_PLAN_MAX_ITEMS),
		Schema.makeFilter((items: ReadonlyArray<DeliveryPlanItem>) =>
			new Set(items.map(({ id }) => id)).size === items.length ? undefined : 'expected unique item ids',
		),
	),
})
export type DeliveryPlan = typeof DeliveryPlan.Type

/** Whether two plans are the same. */
export const sameDeliveryPlan = Schema.toEquivalence(DeliveryPlan)

/** Counts the plans a delivery has been given, from 1. */
export const DeliveryPlanRevision = Schema.Int.check(Schema.isGreaterThan(0))
export type DeliveryPlanRevision = typeof DeliveryPlanRevision.Type

/** Show one revision of the plan. It holds the plan as it was when saved, which is what the provider shows. */
export const RenderPlan = Schema.TaggedStruct('RenderPlan', {
	revision: DeliveryPlanRevision,
	plan: DeliveryPlan,
})
export type RenderPlan = typeof RenderPlan.Type

/**
 * The plan a provider last showed.
 *
 * @property plan - exactly the plan that was sent, not the desired plan at the time it settled
 * @property presentation - the provider's own reference to where it shows the plan, such as a Slack
 * stream or a GitHub comment; read only by that provider and never returned by the delivery API
 */
export const RenderedDeliveryPlan = Schema.Struct({
	revision: DeliveryPlanRevision,
	plan: DeliveryPlan,
	presentation: Schema.optionalKey(Schema.Json),
})
export type RenderedDeliveryPlan = typeof RenderedDeliveryPlan.Type

/**
 * A delivery's plan as the store keeps it.
 *
 * @property desired - the latest plan the remote worker sent
 * @property rendered - the plan a provider last showed; absent until one has
 */
export const DeliveryPlanState = Schema.Struct({
	desired: Schema.Struct({ revision: DeliveryPlanRevision, plan: DeliveryPlan }),
	rendered: Schema.optionalKey(RenderedDeliveryPlan),
})
export type DeliveryPlanState = typeof DeliveryPlanState.Type

/**
 * `RenderPlan` as a provider receives it, with the plan it last showed, if any. A provider that changes
 * its plan step by step compares the two; one that replaces the whole plan ignores `rendered` but for
 * its presentation.
 */
export const ProviderRenderPlan = Schema.TaggedStruct('RenderPlan', {
	revision: DeliveryPlanRevision,
	plan: DeliveryPlan,
	rendered: Schema.optionalKey(RenderedDeliveryPlan),
})
export type ProviderRenderPlan = typeof ProviderRenderPlan.Type

/**
 * What a remote worker may read about its plan.
 *
 * @property revision - the revision of `plan`, the latest the remote worker sent
 * @property renderedRevision - the revision a provider last showed; absent until one has, and always
 * when the destination cannot show plans
 */
export const DeliveryPlanStatus = Schema.Struct({
	revision: DeliveryPlanRevision,
	plan: DeliveryPlan,
	renderedRevision: Schema.optionalKey(DeliveryPlanRevision),
})
export type DeliveryPlanStatus = typeof DeliveryPlanStatus.Type

export const deliveryPlanStatus = (state: DeliveryPlanState) => {
	const status = { revision: state.desired.revision, plan: state.desired.plan }
	return DeliveryPlanStatus.make(
		Predicate.isUndefined(state.rendered) ? status : { ...status, renderedRevision: state.rendered.revision },
	)
}

/** One line of text, for a list item. */
const oneLine = (text: string) => text.replace(/\s*\n\s*/g, ' ')

/** An item as one Markdown list line: a mark for its state, its title, and its note. */
const itemMarkdown = (item: DeliveryPlanItem) => {
	const [mark, note] = Match.value(item.state).pipe(
		Match.tagsExhaustive({
			Pending: () => ['⬜', undefined] as const,
			InProgress: ({ details }) => ['🔄', details] as const,
			Completed: ({ result }) => ['✅', result] as const,
			Failed: ({ reason }) => ['❌', reason] as const,
		}),
	)
	const line = `- ${mark} ${oneLine(item.title)}`
	return Predicate.isUndefined(note) ? line : `${line}: ${oneLine(note)}`
}

/**
 * The plan as Markdown, for a provider that shows it in one comment, such as GitHub. The title, then one
 * line per item with a mark for its state.
 */
export const deliveryPlanMarkdown = (plan: DeliveryPlan) => {
	const lines = Arr.match(plan.items, { onEmpty: () => ['_No steps yet._'], onNonEmpty: Arr.map(itemMarkdown) })
	return [`**${oneLine(plan.title ?? 'Plan')}**`, '', ...lines].join('\n')
}
