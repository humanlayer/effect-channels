import { Effect, Layer, Schema } from 'effect'
import * as SqlClient from 'effect/unstable/sql/SqlClient'

import { SubscriptionInput } from '../Operations'
import { SubscriptionCreated, SubscriptionExisting } from '../SlackEvents'
import { ResolveSlackDirectMessageRoute, SlackDirectMessageRoute, SlackSubscriptions } from '../SlackSubscriptions'
import { subscriptionErrors } from './errors'
import { initialized } from './migrations'

const routeJson = Schema.fromJsonString(SlackDirectMessageRoute)
const createdRows = Schema.Tuple([Schema.Struct({ created: Schema.Boolean })])
const subscribedRows = Schema.Tuple([Schema.Struct({ subscribed: Schema.Boolean })])
const routeRows = Schema.Tuple([Schema.Struct({ route_json: routeJson })])

const cleanup = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	yield* sql`DELETE FROM humanlayer_slack_v1_subscriptions WHERE thread_id IN (
		SELECT thread_id FROM humanlayer_slack_v1_subscriptions
		WHERE expires_at <= statement_timestamp() ORDER BY expires_at, thread_id
		LIMIT 128 FOR UPDATE SKIP LOCKED
	)`
	yield* sql`DELETE FROM humanlayer_slack_v1_routes WHERE (tenant, channel_id, event_id) IN (
		SELECT tenant, channel_id, event_id FROM humanlayer_slack_v1_routes
		WHERE expires_at <= statement_timestamp() ORDER BY expires_at, tenant, channel_id, event_id
		LIMIT 128 FOR UPDATE SKIP LOCKED
	)`
})

const isSubscribed = Effect.fn('slack.postgres.subscriptions.is_subscribed')(
	function* (input: SubscriptionInput) {
		yield* Schema.decodeEffect(SubscriptionInput)(input)
		yield* cleanup
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const [row] = yield* Schema.decodeUnknownEffect(subscribedRows)(
			yield* sql`SELECT EXISTS (
			SELECT 1 FROM humanlayer_slack_v1_subscriptions
			WHERE thread_id = ${input.threadId} AND expires_at > statement_timestamp()
		) AS subscribed`,
		)
		return row.subscribed
	},
	(effect, input) => effect.pipe(subscriptionErrors({ operation: 'isSubscribed', threadId: input.threadId })),
)

const subscribe = Effect.fn('slack.postgres.subscriptions.subscribe')(
	function* (input: SubscriptionInput) {
		yield* Schema.decodeEffect(SubscriptionInput)(input)
		yield* cleanup
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const [row] = yield* Schema.decodeUnknownEffect(createdRows)(
			yield* sql`INSERT INTO humanlayer_slack_v1_subscriptions
			(thread_id, expires_at, created) VALUES (${input.threadId}, statement_timestamp() + interval '720 hours', true)
			ON CONFLICT (thread_id) DO UPDATE SET
				created = humanlayer_slack_v1_subscriptions.expires_at <= statement_timestamp(),
				expires_at = EXCLUDED.expires_at
			RETURNING created`,
		)
		return row.created ? SubscriptionCreated.make({}) : SubscriptionExisting.make({})
	},
	(effect, input) => effect.pipe(subscriptionErrors({ operation: 'subscribe', threadId: input.threadId })),
)

const unsubscribe = Effect.fn('slack.postgres.subscriptions.unsubscribe')(
	function* (input: SubscriptionInput) {
		yield* Schema.decodeEffect(SubscriptionInput)(input)
		yield* cleanup
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		yield* sql`DELETE FROM humanlayer_slack_v1_subscriptions WHERE thread_id = ${input.threadId}`
	},
	(effect, input) => effect.pipe(subscriptionErrors({ operation: 'unsubscribe', threadId: input.threadId })),
)

const resolveDirectMessageRoute = Effect.fn('slack.postgres.subscriptions.resolve_dm_route')(
	function* (input: ResolveSlackDirectMessageRoute) {
		yield* Schema.decodeEffect(ResolveSlackDirectMessageRoute)(input)
		const rooted = yield* Schema.encodeEffect(routeJson)({ thread: input.rootedThread, subscribed: true })
		const proactive = yield* Schema.encodeEffect(routeJson)({ thread: input.proactiveThread, subscribed: true })
		const fallback = yield* Schema.encodeEffect(routeJson)({ thread: input.rootedThread, subscribed: false })
		yield* cleanup
		const sql = (yield* SqlClient.SqlClient).withoutTransforms()
		const [row] = yield* Schema.decodeUnknownEffect(routeRows)(
			yield* sql`INSERT INTO humanlayer_slack_v1_routes
			(tenant, channel_id, event_id, route_json, expires_at)
			VALUES (${input.rootedThread.channel.tenant}, ${input.rootedThread.channel.id}, ${input.eventId},
				CASE WHEN EXISTS (SELECT 1 FROM humanlayer_slack_v1_subscriptions
					WHERE thread_id = ${input.rootedThread.id} AND expires_at > statement_timestamp()) THEN ${rooted}
				WHEN EXISTS (SELECT 1 FROM humanlayer_slack_v1_subscriptions
					WHERE thread_id = ${input.proactiveThread.id} AND expires_at > statement_timestamp()) THEN ${proactive}
				ELSE ${fallback} END,
				statement_timestamp() + interval '24 hours')
			ON CONFLICT (tenant, channel_id, event_id) DO UPDATE SET
				route_json = CASE WHEN humanlayer_slack_v1_routes.expires_at <= statement_timestamp()
					THEN EXCLUDED.route_json ELSE humanlayer_slack_v1_routes.route_json END,
				expires_at = CASE WHEN humanlayer_slack_v1_routes.expires_at <= statement_timestamp()
					THEN EXCLUDED.expires_at ELSE humanlayer_slack_v1_routes.expires_at END
			RETURNING route_json`,
		)
		return row.route_json
	},
	(effect, input) =>
		effect.pipe(subscriptionErrors({ operation: 'resolveDirectMessageRoute', threadId: input.rootedThread.id })),
)

export const subscriptions = Layer.effect(
	SlackSubscriptions,
	Effect.gen(function* () {
		const sql = yield* SqlClient.SqlClient
		return SlackSubscriptions.of({
			isSubscribed: (input) => isSubscribed(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
			subscribe: (input) => subscribe(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
			unsubscribe: (input) => unsubscribe(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
			resolveDirectMessageRoute: (input) =>
				resolveDirectMessageRoute(input).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
		})
	}),
).pipe(Layer.provide(initialized))
