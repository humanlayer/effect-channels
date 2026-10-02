import { NodeRuntime, NodeServices } from '@effect/platform-node'
import { Config, Crypto, Data, Effect, Option, Path, Schedule, Schema, Stream } from 'effect'
import { ChildProcess, ChildProcessSpawner } from 'effect/unstable/process'

class BackendTestFailure extends Data.TaggedError('BackendTestFailure')<{ readonly operation: string }> {}

const command = Effect.fn('backend_test.command')(function* (input: {
	readonly executable: string
	readonly args: ReadonlyArray<string>
	readonly cwd: string
	readonly env?: Record<string, string>
}) {
	const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
	return yield* Effect.scoped(
		Effect.gen(function* () {
			const process = yield* spawner.spawn(
				ChildProcess.make(input.executable, input.args, {
					cwd: input.cwd,
					env: input.env,
					extendEnv: true,
					stdin: 'ignore',
					stdout: 'pipe',
					stderr: 'pipe',
					forceKillAfter: '1 second',
				}),
			)
			const [output, code] = yield* Effect.all(
				[
					process.all.pipe(
						Stream.decodeText(),
						Stream.runFold(
							() => '',
							(text, chunk) => (text + chunk).slice(-10_000),
						),
					),
					process.exitCode,
				],
				{ concurrency: 2 },
			)
			if (code !== 0) {
				yield* Effect.logError('Backend test command failed', output)
				return yield* new BackendTestFailure({ operation: input.executable })
			}
			return output
		}),
	).pipe(Effect.timeout('45 seconds'))
})

const program = Effect.gen(function* () {
	const backend = yield* Config.schema(Schema.Literals(['postgres', 'redis']), 'PHASE2_BACKEND')
	const path = yield* Path.Path
	const root = yield* path.fromFileUrl(new URL('../', import.meta.url))
	const crypto = yield* Crypto.Crypto
	const override = yield* Config.option(Config.string('DOCKER_HOST'))
	const endpoint = yield* command({
		executable: 'docker',
		args: ['context', 'inspect', '--format', '{{.Endpoints.docker.Host}}'],
		cwd: root,
	})
	if (!endpoint.trim().startsWith('unix://') || (Option.isSome(override) && !override.value.startsWith('unix://'))) {
		return yield* new BackendTestFailure({
			operation: 'A local Unix-socket Docker daemon is required; remote contexts are refused',
		})
	}
	const suites =
		backend === 'postgres'
			? [{ config: 'packages/sql/test-backends/vite.postgres.config.ts', port: 55432 }]
			: [{ config: 'packages/redis/test-backends/vite.redis.config.ts', port: 56379 }]
	for (const suite of suites) {
		yield* Effect.scoped(
			Effect.gen(function* () {
				const name = `channels-test-${yield* crypto.randomUUIDv4}`
				const docker = (args: ReadonlyArray<string>) => command({ executable: 'docker', args, cwd: root })
				const options =
					backend === 'postgres'
						? [
								'-p',
								`127.0.0.1:${suite.port}:5432`,
								'-e',
								'POSTGRES_USER=delivery_test',
								'-e',
								'POSTGRES_PASSWORD=delivery_test',
								'-e',
								'POSTGRES_DB=delivery_adapter_test',
								'postgres:17-alpine',
							]
						: [
								'-p',
								`127.0.0.1:${suite.port}:6379`,
								'redis:7-alpine',
								'redis-server',
								'--appendonly',
								'yes',
								'--appendfsync',
								'always',
								'--maxmemory-policy',
								'noeviction',
							]
				yield* Effect.acquireRelease(docker(['create', '--name', name, ...options]), () =>
					docker(['rm', '-fv', name]).pipe(
						Effect.tapError(() =>
							Effect.logError('Could not remove owned backend test container', { name }),
						),
						Effect.orDie,
					),
				)
				yield* docker(['start', name])
				yield* docker([
					'exec',
					name,
					...(backend === 'postgres'
						? ['pg_isready', '-h', '127.0.0.1', '-U', 'delivery_test', '-d', 'delivery_adapter_test']
						: ['redis-cli', 'ping']),
				]).pipe(Effect.retry({ times: 20, schedule: Schedule.spaced('250 millis') }))
				yield* Effect.logInfo(`Testing ${suite.config} with a fresh ${backend} container`)
				const result = yield* command({
					executable: path.join(root, 'node_modules/.bin/vp'),
					args: ['test', '--config', suite.config],
					cwd: root,
					env: {
						DELIVERY_BACKEND_TEST_CONFIRM: 'disposable',
						...(backend === 'postgres'
							? {
									SQL_BACKEND_TEST_DATABASE_URL: `postgres://delivery_test:delivery_test@127.0.0.1:${suite.port}/delivery_adapter_test`,
								}
							: { REDIS_CONTRACT_TEST_PORT: String(suite.port) }),
					},
				})
				yield* Effect.logInfo(result)
			}),
		)
	}
}).pipe(Effect.scoped, Effect.timeout('115 seconds'), Effect.provide(NodeServices.layer))

NodeRuntime.runMain(program)
