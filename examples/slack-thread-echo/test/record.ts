import { Clock, Config, Context, Effect, FileSystem, Layer, Redacted, Ref, Schema } from 'effect'
import { HttpClient, HttpClientRequest, HttpServerRequest } from 'effect/unstable/http'

type Exchange = Readonly<Record<string, string | number>>

const TOKEN = /xox[baprs]-[A-Za-z0-9-]+/g
const SLACK_ID = /\b([TUCWBAEG])[A-Z0-9]{8,}\b/g
const EMAIL = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi
const SENSITIVE_VALUE = /("(?:text|real_name|display_name|email|image_192)"\s*:\s*)"(?:\\.|[^"\\])*"/g
const ESCAPED_SENSITIVE_VALUE =
	/(\\["](?:text|real_name|display_name|email|image_192)\\["]\s*:\s*)\\["](?:\\.|[^"\\])*\\["]/g

const sanitize = (input: string, secrets: ReadonlyArray<string>) => {
	let output = input
	for (const secret of secrets) {
		output = output.replaceAll(secret, '[REDACTED]')
	}
	return output
		.replace(TOKEN, '[REDACTED_TOKEN]')
		.replace(SENSITIVE_VALUE, '$1"[SANITIZED]"')
		.replace(ESCAPED_SENSITIVE_VALUE, '$1\\"[SANITIZED]\\"')
		.replace(EMAIL, '[SANITIZED_EMAIL]')
		.replace(SLACK_ID, '$1_RECORDED')
}

export class SlackRecording extends Context.Service<
	SlackRecording,
	{
		readonly enabled: boolean
		readonly append: (exchange: Exchange) => Effect.Effect<void>
		readonly write: Effect.Effect<string>
	}
>()('example/test/SlackRecording') {
	static layer(enabled: boolean) {
		return Layer.effect(
			SlackRecording,
			Effect.gen(function* () {
				const fs = yield* FileSystem.FileSystem
				const signingSecret = yield* Config.redacted('SLACK_SIGNING_SECRET')
				const botToken = yield* Config.redacted('SLACK_BOT_TOKEN')
				const outputDirectory = yield* Config.string('SLACK_RECORDING_DIR').pipe(
					Config.withDefault('.recordings'),
				)
				const exchanges = yield* Ref.make<ReadonlyArray<Exchange>>([])
				const secrets = [Redacted.value(signingSecret), Redacted.value(botToken)]
				return SlackRecording.of({
					enabled,
					append: (exchange) =>
						enabled ? Ref.update(exchanges, (items) => [...items, exchange]) : Effect.void,
					write: Effect.gen(function* () {
						const recordedAt = yield* Clock.currentTimeMillis
						const raw = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))({
							format: 'humanlayer-slack-recording-v1',
							recordedAt,
							exchanges: yield* Ref.get(exchanges),
						}).pipe(Effect.orDie)
						const output = sanitize(raw, secrets)
						if (TOKEN.test(output) || secrets.some((secret) => output.includes(secret))) {
							return yield* Effect.die(new Error('Slack recording secret scan failed'))
						}
						yield* fs.makeDirectory(outputDirectory, { recursive: true }).pipe(Effect.orDie)
						const path = `${outputDirectory}/slack-acceptance.json`
						yield* fs.writeFileString(path, output).pipe(Effect.orDie)
						return path
					}),
				})
			}),
		)
	}
}

export const RecordingHttpClient = Layer.effect(
	HttpClient.HttpClient,
	Effect.gen(function* () {
		const client = yield* HttpClient.HttpClient
		const recording = yield* SlackRecording
		return HttpClient.make((request) =>
			Effect.gen(function* () {
				const webRequest = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
				const response = yield* client.execute(request)
				yield* recording.append({
					direction: 'outbound',
					method: webRequest.method,
					url: webRequest.url,
					requestBody: yield* Effect.promise(() => webRequest.text()),
					responseStatus: response.status,
					responseBody: yield* response.text,
				})
				return response
			}),
		)
	}),
)

export const recordInbound = <A, E, R>(effect: Effect.Effect<A, E, R | HttpServerRequest.HttpServerRequest>) =>
	Effect.gen(function* () {
		const recording = yield* SlackRecording
		const request = yield* HttpServerRequest.HttpServerRequest
		yield* recording.append({
			direction: 'inbound',
			method: request.method,
			url: request.url,
			body: yield* request.text,
		})
		return yield* effect
	})
