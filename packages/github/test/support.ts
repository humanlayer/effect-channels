import { createHmac } from 'node:crypto'
import { createServer } from 'node:http'

import { NodeHttpServer } from '@effect/platform-node'
import { Context, Effect, Layer, Predicate, Queue, Redacted, Schema } from 'effect'
import {
	FetchHttpClient,
	HttpClient,
	HttpClientRequest,
	HttpClientResponse,
	HttpRouter,
	HttpServer,
	HttpServerResponse,
} from 'effect/unstable/http'
import { createEmulator } from 'emulate'

import { GitHubCredentials, GitHubCrypto, GitHubRepository, GitHubActivityEvent, GitHubIssueData } from '../src/index'

export const secret = 'github-test-webhook-secret'
export const availablePort = Effect.promise(
	() =>
		new Promise<number>((resolve, reject) => {
			const server = createServer()
			server.once('error', reject)
			server.listen(0, '127.0.0.1', () => {
				const address = server.address()
				if (address === null || !Predicate.hasProperty(address, 'port') || !Predicate.isNumber(address.port)) {
					server.close()
					reject(new Error('No TCP address'))
					return
				}
				const port = address.port
				server.close((error) => (error ? reject(error) : resolve(port)))
			})
		}),
)
export const adminCall = <S extends Schema.Top>(url: string, path: string, schema: S, body?: Schema.Json) =>
	Effect.gen(function* () {
		const client = yield* HttpClient.HttpClient
		const request = HttpClientRequest.make(body === undefined ? 'GET' : 'POST')(`${url}${path}`).pipe(
			HttpClientRequest.bearerToken('test_token_admin'),
		)
		const response = yield* client.execute(
			body === undefined ? request : yield* HttpClientRequest.bodyJson(request, body),
		)
		if (response.status >= 300)
			return yield* Effect.fail(new Error(`Emulator status ${response.status} at ${path}`))
		return yield* HttpClientResponse.schemaBodyJson(schema)(response)
	})

export const emulator = (webhookUrl?: string) =>
	Effect.gen(function* () {
		const port = yield* availablePort
		const resource = yield* Effect.acquireRelease(
			Effect.promise(() =>
				createEmulator({
					service: 'github',
					port,
					seed: {
						github: {
							users: [{ login: 'alice' }],
							repos: [
								{ owner: 'alice', name: 'project', auto_init: true },
								{ owner: 'alice', name: 'second', auto_init: true },
							],
							apps: [
								{
									app_id: 42,
									slug: 'channels',
									name: 'Channels',
									permissions: { issues: 'write', pull_requests: 'read' },
									events: [
										'issues',
										'issue_comment',
										'pull_request',
										'pull_request_review',
										'pull_request_review_comment',
										'pull_request_review_thread',
									],
									webhook_url: webhookUrl,
									webhook_secret: secret,
									installations: [
										{
											installation_id: 100,
											account: 'alice',
											repositories: ['project', 'second'],
											repository_selection: 'selected',
										},
									],
								},
							],
						},
					},
				}),
			),
			(resource) => Effect.promise(() => resource.close()),
		)
		const key = resource.generatedSecrets.find((entry) => entry.id === '42')?.value
		if (key === undefined) return yield* Effect.fail(new Error('Missing emulator generated App key'))
		const repo = yield* adminCall(resource.url, '/repos/alice/project', Schema.Struct({ id: Schema.Int }))
		const repository = GitHubRepository.make({
			kind: 'github.repository',
			installationId: 100,
			id: repo.id,
			owner: 'alice',
			name: 'project',
		})
		const credentials = GitHubCredentials.layer({
			appId: 42,
			privateKey: Redacted.make(key),
			installationIds: [100],
			botUserId: 999_999,
			apiUrl: resource.url,
		}).pipe(Layer.provideMerge(GitHubCrypto.layerWebCrypto), Layer.provideMerge(FetchHttpClient.layer))
		return { resource, repository, credentials }
	})
export const eventFor = (
	repository: GitHubRepository,
	issue: GitHubIssueData,
	deliveryId = 'delivery-1',
): Extract<GitHubActivityEvent, { event: 'issues'; action: 'opened' }> => ({
	event: 'issues',
	action: 'opened',
	deliveryId,
	resource: { kind: 'github.issue', repository, number: issue.number },
	issue,
	sender: issue.user,
})
export const payloadFor = (event: GitHubActivityEvent) => ({
	action: event.action,
	installation: { id: event.resource.repository.installationId },
	repository: {
		id: event.resource.repository.id,
		name: event.resource.repository.name,
		owner: { login: event.resource.repository.owner },
	},
	issue: event.event === 'issues' || event.event === 'issue_comment' ? event.issue : undefined,
	pull_request: event.event === 'issues' || event.event === 'issue_comment' ? undefined : event.pull_request,
	sender: event.sender,
	comment: event.event === 'issue_comment' ? event.comment : undefined,
})
export const signedRequest = (event: string, body: string | Uint8Array<ArrayBuffer>, deliveryId = 'delivery-1') =>
	new Request('http://test/integrations/github/webhook', {
		method: 'POST',
		body,
		headers: {
			'content-type': 'application/json',
			'x-github-event': event,
			'x-github-delivery': deliveryId,
			'x-hub-signature-256': `sha256=${createHmac('sha256', secret).update(body).digest('hex')}`,
		},
	})
export const host = <E>(routes: Layer.Layer<never, E, import('effect/unstable/http/HttpRouter').HttpRouter>) =>
	Effect.gen(function* () {
		const web = HttpRouter.toWebHandler(routes, { disableLogger: true })
		yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
		return (request: Request) => Effect.promise(() => web.handler(request, Context.empty()))
	})

export const captureWebhooks = Effect.gen(function* () {
	const requests = yield* Queue.unbounded<Request>()
	const capture = HttpRouter.add('POST', '/capture', (request) =>
		Effect.gen(function* () {
			const body = yield* request.text
			yield* Queue.offer(
				requests,
				new Request('http://test/integrations/github/webhook', {
					method: 'POST',
					body,
					headers: request.headers,
				}),
			)
			return HttpServerResponse.empty({ status: 200 })
		}),
	)
	const context = yield* Layer.build(
		HttpRouter.serve(capture).pipe(
			Layer.provideMerge(NodeHttpServer.layer(createServer, { port: 0, host: '127.0.0.1' })),
		),
	)
	const address = Context.get(context, HttpServer.HttpServer).address
	if (!Predicate.isTagged(address, 'TcpAddress')) return yield* Effect.die('Expected TCP address')
	return { url: `http://127.0.0.1:${address.port}/capture`, take: Queue.take(requests) }
})
