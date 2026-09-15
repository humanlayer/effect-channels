import { createHmac } from 'node:crypto'
import * as NodeHttp from 'node:http'

import { NodeCrypto } from '@effect/platform-node'
import { createServer, serve } from '@emulators/core'
import { getGitHubStore, githubPlugin, seedFromConfig, type GitHubSeedConfig } from '@emulators/github'
import {
	DeliveryAdmission,
	DeliveryReceipt,
	MailboxDelivery,
	processProviderEvent,
	type ProviderEventProcessor,
	webhookRoutes,
	type RawWebhookInput,
} from '@humanlayer/channels-delivery-next'
import { Context, Effect, Match, Queue, Redacted, Schema } from 'effect'
import { Headers, HttpRouter } from 'effect/unstable/http'

import { makeGitHubWebhookProvider } from '../src/GitHubWebhookProvider'

export const githubWebhookSecret = 'github-next-test-secret'
export const githubEmulatorAliceToken = 'github-next-alice-token'
export const githubEmulatorReviewerToken = 'github-next-reviewer-token'

const inMemoryMailboxKey = (admission: DeliveryAdmission) =>
	[admission.namespace, admission.provider, admission.installationId, admission.resourceId]
		.map((segment) => `${segment.length}:${segment}`)
		.join('|')

/** Test-only keyed mailbox that stores admissions until the test explicitly processes one. */
export const makeInMemoryMailboxFixture = <R>(processors: ReadonlyArray<ProviderEventProcessor<R>>) =>
	Effect.gen(function* () {
		const mailboxes = new Map<string, Queue.Queue<DeliveryAdmission>>()
		const createdMailboxKeys = yield* Queue.unbounded<string>()

		const mailboxDelivery: typeof MailboxDelivery.Service = {
			deliver: (admission) =>
				Effect.gen(function* () {
					const key = inMemoryMailboxKey(admission)
					let mailbox = mailboxes.get(key)
					if (mailbox === undefined) {
						mailbox = yield* Queue.unbounded<DeliveryAdmission>()
						mailboxes.set(key, mailbox)
						yield* Queue.offer(createdMailboxKeys, key)
					}
					yield* Queue.offer(mailbox, admission)
					return DeliveryReceipt.make({ mailboxKey: key, accepted: true })
				}),
		}

		return {
			mailboxDelivery,
			awaitMailboxKey: Queue.take(createdMailboxKeys),
			processNext: (mailboxKey: string) => {
				const mailbox = mailboxes.get(mailboxKey)
				return mailbox === undefined
					? Effect.die(new Error(`In-memory mailbox not found: ${mailboxKey}`))
					: Queue.take(mailbox).pipe(Effect.flatMap(processProviderEvent(processors)))
			},
		}
	})

const githubUser = { id: 400, login: 'alice', type: 'User' }

export const issuePayload = (action: string) => ({
	action,
	installation: { id: 100 },
	repository: { id: 200, name: 'project', owner: { login: 'alice' } },
	issue: {
		id: 300,
		number: 42,
		title: 'Test issue',
		body: 'Issue body',
		state: 'open',
		html_url: 'https://github.com/alice/project/issues/42',
		user: githubUser,
	},
	sender: { id: 401, login: 'bob', type: 'User' },
})

export const issueCommentPayload = (input?: {
	readonly action?: string
	readonly issueNumber?: number
	readonly pullRequest?: boolean
}) => ({
	action: input?.action ?? 'created',
	installation: { id: 100 },
	repository: { id: 200, name: 'project', owner: { login: 'alice' } },
	issue: {
		id: 300,
		number: input?.issueNumber ?? 42,
		title: 'Test discussion',
		body: 'Issue body',
		state: 'open',
		html_url: 'https://github.com/alice/project/issues/42',
		user: githubUser,
		...(input?.pullRequest === true
			? { pull_request: { url: 'https://api.github.com/repos/alice/project/pulls/42' } }
			: {}),
	},
	comment: {
		id: 500,
		body: '@agent please review',
		html_url: 'https://github.com/alice/project/issues/42#issuecomment-500',
		user: { id: 401, login: 'bob', type: 'User' },
	},
	sender: { id: 401, login: 'bob', type: 'User' },
})

export const pullRequestPayload = (action = 'opened') => ({
	action,
	installation: { id: 100 },
	repository: { id: 200, name: 'project', owner: { login: 'alice' } },
	pull_request: {
		id: 301,
		number: 42,
		title: 'Test pull request',
		body: 'Pull request body',
		state: 'open',
		html_url: 'https://github.com/alice/project/pull/42',
		user: githubUser,
		draft: false,
		head: { ref: 'feature', sha: 'abc123' },
		base: { ref: 'main', sha: 'def456' },
	},
	sender: githubUser,
})

export const pullRequestReviewPayload = (action = 'submitted') => ({
	...pullRequestPayload(),
	action,
	review: {
		id: 600,
		node_id: 'PRR_600',
		body: 'Looks good',
		user: githubUser,
		state: 'approved',
		commit_id: 'abc123',
		html_url: 'https://github.com/alice/project/pull/42#pullrequestreview-600',
	},
})

export const pullRequestReviewCommentPayload = (action = 'created') => ({
	...pullRequestPayload(),
	action,
	comment: {
		id: 700,
		node_id: 'PRRC_700',
		body: 'Change this',
		html_url: 'https://github.com/alice/project/pull/42#discussion_r700',
		user: githubUser,
		pull_request_review_id: 600,
		path: 'src/index.ts',
		commit_id: 'abc123',
		original_commit_id: 'abc123',
		diff_hunk: '@@ -1 +1 @@',
		pull_request_url: 'https://api.github.com/repos/alice/project/pulls/42',
		line: 1,
		side: 'RIGHT',
	},
})

export const pullRequestReviewThreadPayload = (action = 'resolved') => ({
	...pullRequestPayload(),
	action,
	thread: { node_id: 'PRRT_800', comments: [pullRequestReviewCommentPayload().comment] },
})

export const pullRequestIssueCommentPayload = () => {
	const payload = issueCommentPayload({ pullRequest: true })
	return {
		...payload,
		issue: {
			...payload.issue,
			title: 'PR',
			body: 'body',
			html_url: 'https://github.com/alice/project/pull/42',
		},
		comment: {
			id: 500,
			body: 'comment',
			html_url: 'https://github.com/alice/project/pull/42#issuecomment-500',
			user: githubUser,
		},
		sender: githubUser,
	}
}

export const makeGitHubTestProvider = (namespace: string) => {
	const provider = makeGitHubWebhookProvider({ namespace, webhookSecret: Redacted.make(githubWebhookSecret) })
	return {
		handle: (input: RawWebhookInput) => provider.handle(input).pipe(Effect.provide(NodeCrypto.layer)),
	}
}

export const githubEmulatorRequest = (
	url: string,
	path: string,
	token: string,
	options: { readonly method?: string; readonly body?: unknown } = {},
) =>
	Effect.promise(async () => {
		const response = await fetch(`${url}${path}`, {
			method: options.method ?? 'POST',
			headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
			body: options.body === undefined ? undefined : JSON.stringify(options.body),
		})
		if (!response.ok)
			throw new Error(`GitHub emulator returned ${response.status} for ${path}: ${await response.text()}`)
		if (response.status === 204) return undefined
		const json: unknown = await response.json()
		return json
	})

export const decodeGitHubNumberedResponse = Schema.decodeUnknownEffect(Schema.Struct({ number: Schema.Number }))

export const decodeGitHubIdentifiedResponse = Schema.decodeUnknownEffect(Schema.Struct({ id: Schema.Number }))

const listen = (server: NodeHttp.Server) =>
	Effect.callback<number, Error>((resume) => {
		server.once('error', (cause) => resume(Effect.fail(cause)))
		server.listen(0, '127.0.0.1', () => {
			const address = server.address()
			if (address === null || typeof address === 'string') {
				resume(Effect.fail(new Error('GitHub emulator server did not expose a TCP port')))
				return
			}
			resume(Effect.succeed(address.port))
		})
	})

const close = (server: NodeHttp.Server) =>
	Effect.promise(
		() =>
			new Promise<void>((resolve, reject) => {
				server.close((cause) => (cause === undefined ? resolve() : reject(cause)))
			}),
	)

const requestListener =
	(handler: (request: Request) => Promise<Response>): NodeHttp.RequestListener =>
	(request, response) => {
		const chunks: Array<Uint8Array> = []
		request.on('data', (chunk: Uint8Array) => chunks.push(chunk))
		request.on('end', () => {
			const headers = new globalThis.Headers()
			for (const [name, value] of Object.entries(request.headers)) {
				if (Array.isArray(value)) for (const item of value) headers.append(name, item)
				else if (value !== undefined) headers.set(name, value)
			}
			const body = Buffer.concat(chunks)
			void handler(
				new Request(`http://${request.headers.host ?? '127.0.0.1'}${request.url ?? '/'}`, {
					method: request.method,
					headers,
					body: body.length === 0 ? undefined : body,
				}),
			).then(
				async (result) => {
					response.writeHead(result.status, Object.fromEntries(result.headers))
					response.end(Buffer.from(await result.arrayBuffer()))
				},
				() => {
					response.writeHead(500)
					response.end()
				},
			)
		})
	}

export type GitHubEmulatorFixtureOptions = {
	readonly mailboxDelivery: typeof MailboxDelivery.Service
}

export const makeGitHubEmulatorFixture = (options: GitHubEmulatorFixtureOptions) =>
	Effect.gen(function* () {
		const provider = makeGitHubWebhookProvider({
			namespace: 'github-emulator-test',
			webhookSecret: Redacted.make(githubWebhookSecret),
		})
		const routes = webhookRoutes([provider]).pipe(HttpRouter.provideRequest(NodeCrypto.layer))
		const web = HttpRouter.toWebHandler(routes, { disableLogger: true })
		yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
		const context = Context.make(MailboxDelivery, options.mailboxDelivery)
		const callbackServer = NodeHttp.createServer(
			requestListener(async (request) => {
				return web.handler(request, context)
			}),
		)
		const callbackPort = yield* listen(callbackServer)
		yield* Effect.addFinalizer(() => close(callbackServer))

		const emulator = createServer(githubPlugin, { baseUrl: 'http://127.0.0.1' })
		githubPlugin.seed?.(emulator.store, emulator.baseUrl)
		const seed = {
			users: [{ login: 'alice' }, { login: 'reviewer' }],
			tokens: {
				[githubEmulatorAliceToken]: { login: 'alice' },
				[githubEmulatorReviewerToken]: { login: 'reviewer' },
			},
			repos: [{ owner: 'alice', name: 'project', auto_init: true }],
			apps: [
				{
					app_id: 42,
					slug: 'github-next',
					name: 'GitHub Next Test App',
					private_key: 'github-next-test-private-key',
					permissions: { issues: 'write', pull_requests: 'write' },
					events: [
						'issues',
						'issue_comment',
						'pull_request',
						'pull_request_review',
						'pull_request_review_comment',
					],
					webhook_url: `http://127.0.0.1:${callbackPort}/integrations/github/webhook`,
					webhook_secret: githubWebhookSecret,
					installations: [
						{
							installation_id: 100,
							account: 'alice',
							repositories: ['project'],
							repository_selection: 'selected',
						},
					],
				},
			],
		} satisfies GitHubSeedConfig
		seedFromConfig(emulator.store, emulator.baseUrl, seed)

		const emulatorServer = serve({ fetch: emulator.app.fetch, hostname: '127.0.0.1', port: 0 })
		const emulatorPort = yield* listen(emulatorServer)
		yield* Effect.addFinalizer(() => close(emulatorServer))

		const store = getGitHubStore(emulator.store)
		const alice = store.users.findOneBy('login', 'alice')
		const reviewer = store.users.findOneBy('login', 'reviewer')
		const repository = store.repos.findOneBy('name', 'project')
		const installation = store.appInstallations.findOneBy('installation_id', 100)
		if (alice === undefined || reviewer === undefined || repository === undefined || installation === undefined) {
			return yield* Effect.fail(new Error('GitHub emulator fixture seed is incomplete'))
		}
		emulator.tokenMap.set(githubEmulatorAliceToken, { login: alice.login, id: alice.id, scopes: [] })
		emulator.tokenMap.set(githubEmulatorReviewerToken, { login: reviewer.login, id: reviewer.id, scopes: [] })

		return {
			url: `http://127.0.0.1:${emulatorPort}`,
			aliceToken: githubEmulatorAliceToken,
			reviewerToken: githubEmulatorReviewerToken,
			aliceUserId: alice.id,
			reviewerUserId: reviewer.id,
			repositoryId: repository.id,
			installationId: installation.installation_id,
		}
	})

export const signedGitHubInput = (event: string, payload: unknown, deliveryId = 'delivery-1'): RawWebhookInput =>
	signedGitHubBody(event, new TextEncoder().encode(JSON.stringify(payload)), deliveryId)

export const signedGitHubBody = (event: string, body: Uint8Array, deliveryId = 'delivery-1'): RawWebhookInput => {
	const signature = createHmac('sha256', githubWebhookSecret).update(body).digest('hex')
	return {
		headers: Headers.fromInput({
			'x-github-delivery': deliveryId,
			'x-github-event': event,
			'x-hub-signature-256': `sha256=${signature}`,
		}),
		body,
	}
}

export const admitStoredGitHubWebhook = (namespace: string, event: string, payload: unknown) =>
	Effect.gen(function* () {
		const outcome = yield* makeGitHubTestProvider(namespace).handle(signedGitHubInput(event, payload))
		const admission = yield* Match.value(outcome).pipe(
			Match.tagsExhaustive({
				Event: ({ event }) => Effect.succeed(event),
				Ignored: () =>
					Effect.die(new Error(`Expected GitHub ${event} webhook to be admitted, but it was ignored`)),
				Response: () =>
					Effect.die(new Error(`Expected GitHub ${event} webhook to be admitted, but received a response`)),
			}),
		)
		const codec = Schema.fromJsonString(DeliveryAdmission)
		const encoded = yield* Schema.encodeEffect(codec)(admission)
		return yield* Schema.decodeEffect(codec)(encoded)
	})
