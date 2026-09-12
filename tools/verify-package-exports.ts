import { NodeRuntime, NodeServices } from '@effect/platform-node'
import { Config, Data, Effect, FileSystem, Path, Schema, Stream } from 'effect'
import { ChildProcess, ChildProcessSpawner } from 'effect/unstable/process'

class PackagingFailure extends Data.TaggedError('PackagingFailure')<{
	readonly check: string
	readonly detail: string
}> {
	override get message() {
		return `${this.check}: ${this.detail}`
	}
}

const Versions = Schema.Record(Schema.String, Schema.String)
const Manifest = Schema.Struct({
	name: Schema.String,
	version: Schema.optionalKey(Schema.String),
	dependencies: Schema.optionalKey(Versions),
	devDependencies: Schema.optionalKey(Versions),
	peerDependencies: Schema.optionalKey(Versions),
	peerDependenciesMeta: Schema.optionalKey(
		Schema.Record(Schema.String, Schema.Struct({ optional: Schema.optionalKey(Schema.Boolean) })),
	),
	optionalDependencies: Schema.optionalKey(Versions),
	catalog: Schema.optionalKey(Versions),
})
type Manifest = typeof Manifest.Type
const ConsumerManifest = Schema.Struct({
	name: Schema.String,
	private: Schema.Literal(true),
	type: Schema.Literal('module'),
	dependencies: Versions,
	devDependencies: Versions,
})
const PublishedExports = Schema.Struct({
	exports: Schema.Record(
		Schema.String,
		Schema.Struct({
			types: Schema.String,
			import: Schema.String,
			default: Schema.String,
		}),
	),
})

const BundleImport = Schema.Struct({ path: Schema.String, external: Schema.optionalKey(Schema.Boolean) })
const Metafile = Schema.Struct({
	inputs: Schema.Record(Schema.String, Schema.Struct({ imports: Schema.Array(BundleImport) })),
	outputs: Schema.Record(
		Schema.String,
		Schema.Struct({
			imports: Schema.Array(BundleImport),
			inputs: Schema.Record(Schema.String, Schema.Struct({ bytesInOutput: Schema.Natural })),
		}),
	),
})
const NodeInfo = Schema.Struct({ executable: Schema.String, platform: Schema.String, arch: Schema.String })
const packageDirectories = ['delivery', 'slack', 'github'] as const
const expectedEntries = ['.', './memory', './postgres', './redis', './postgres/client', './redis/client'] as const
const deliveryEntries = [...expectedEntries, './contract', './client', './protocol', './server'] as const
const browserEntries = [
	'browser-delivery',
	'browser-slack',
	'browser-outbound',
	'browser-github',
	'browser-github-outbound',
] as const
const forbiddenBrowserInput =
	/(?:node:|\/node_modules\/(?:@effect\/(?:sql-[^/]+|platform-node[^/]*)|@redis|redis|ioredis|pg(?:-[^/]+)?|postgres|alchemy)\/|\/unstable\/(?:sql\/|persistence\/Redis)|\/(?:postgres|redis)\/|\/(?:postgres|redis)\.js$)/i

const expect = (condition: boolean, check: string, detail: string) =>
	condition ? Effect.void : Effect.fail(new PackagingFailure({ check, detail }))

const readManifest = Effect.fn('packaging.readManifest')(function* (file: string) {
	const fs = yield* FileSystem.FileSystem
	return yield* Schema.decodeEffect(Schema.fromJsonString(Manifest))(yield* fs.readFileString(file))
})

interface CommandTask {
	readonly check: string
	readonly executable: string
	readonly args: ReadonlyArray<string>
	readonly cwd: string
	readonly env: Record<string, string>
	readonly seconds: 30 | 90
}

const command = Effect.fn('packaging.command')(function* (task: CommandTask) {
	yield* Effect.logInfo(`Checking: ${task.check}`)
	const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
	return yield* Effect.scoped(
		Effect.gen(function* () {
			const child = yield* spawner.spawn(
				ChildProcess.make(task.executable, task.args, {
					cwd: task.cwd,
					env: task.env,
					extendEnv: false,
					shell: false,
					stdin: 'ignore',
					stdout: 'pipe',
					stderr: 'pipe',
					forceKillAfter: '1 second',
				}),
			)
			const [output, code] = yield* Effect.all(
				[
					child.all.pipe(
						Stream.decodeText(),
						Stream.runFold(
							() => '',
							(tail, text) => (tail + text).slice(-16_000),
						),
					),
					child.exitCode,
				],
				{ concurrency: 2 },
			)
			yield* expect(code === 0, task.check, `Exit ${code}\n${output}`)
			return output
		}),
	).pipe(Effect.timeout(`${task.seconds} seconds`))
})

const checkAncestors = Effect.fn('packaging.checkAncestors')(function* (directory: string) {
	const fs = yield* FileSystem.FileSystem
	const path = yield* Path.Path
	let current = path.dirname(directory)
	while (true) {
		yield* expect(
			!(yield* fs.exists(path.join(current, 'node_modules'))),
			'consumer isolation',
			'A temporary consumer ancestor contains node_modules',
		)
		const parent = path.dirname(current)
		if (parent === current) return
		current = parent
	}
})

const checkInstalled = Effect.fn('packaging.checkInstalled')(function* (
	consumer: string,
	packages: ReadonlyArray<Manifest>,
	effectVersion: string,
	withPeers: boolean,
) {
	const fs = yield* FileSystem.FileSystem
	const path = yield* Path.Path
	const modules = path.join(consumer, 'node_modules')
	const entries = yield* fs.readDirectory(modules, { recursive: true })
	const effectManifests = entries.filter((entry) =>
		entry.replaceAll('\\', '/').match(/(?:^|\/)effect\/package\.json$/),
	)
	yield* expect(
		effectManifests.length === 1,
		'single Effect installation',
		`Expected one effect/package.json, found ${effectManifests.length}`,
	)
	const effect = yield* readManifest(path.join(modules, 'effect', 'package.json'))
	yield* expect(
		effect.version === effectVersion,
		'Effect version',
		`Expected ${effectVersion}, found ${effect.version}`,
	)
	for (const source of packages) {
		const directory = path.join(modules, source.name)
		const copies = entries.filter((entry) => entry.replaceAll('\\', '/').endsWith(`${source.name}/package.json`))
		yield* expect(
			copies.length === 1,
			'single package identity',
			`${source.name}: ${copies.length} installed copies`,
		)
		yield* expect(
			(yield* fs.realPath(directory)) === directory,
			'packed package isolation',
			`${source.name} is a symlink`,
		)
		const installed = yield* readManifest(path.join(directory, 'package.json'))
		const published = yield* Schema.decodeEffect(Schema.fromJsonString(PublishedExports))(
			yield* fs.readFileString(path.join(directory, 'package.json')),
		)
		yield* expect(
			installed.name === source.name && installed.version === source.version,
			'packed manifest',
			source.name,
		)
		let entriesForPackage: ReadonlyArray<string> = expectedEntries
		switch (source.name) {
			case '@humanlayer/channels-github':
				entriesForPackage = ['.', './memory']
				break
			case '@humanlayer/channels-delivery':
				entriesForPackage = deliveryEntries
				break
		}
		yield* expect(Object.keys(published.exports).length === entriesForPackage.length, 'export map', source.name)
		for (const entry of entriesForPackage) {
			const target = published.exports[entry]
			if (target === undefined)
				return yield* new PackagingFailure({
					check: 'export map',
					detail: `${source.name} is missing ${entry}`,
				})
			const stem = entry === '.' ? 'index' : entry.slice(2)
			yield* expect(
				target.import === `./dist/${stem}.js` &&
					target.default === target.import &&
					target.types === `./dist/${stem}.d.ts`,
				'built export targets',
				`${source.name}${entry}`,
			)
			for (const file of [target.import, target.types]) {
				yield* expect(
					yield* fs.exists(path.join(directory, file)),
					'packed export file',
					`${source.name}/${file}`,
				)
			}
		}
		for (const [name, version] of Object.entries(installed.dependencies ?? {})) {
			yield* expect(
				!version.startsWith('workspace:') && !version.startsWith('catalog:'),
				'packed dependency protocol',
				`${source.name} -> ${name}`,
			)
		}
		if (!withPeers) {
			for (const [peer, metadata] of Object.entries(source.peerDependenciesMeta ?? {})) {
				if (metadata.optional === true) {
					const suffix = `${peer}/package.json`
					yield* expect(
						!entries.some((entry) => entry.replaceAll('\\', '/').endsWith(suffix)),
						'optional peer absent',
						peer,
					)
				}
			}
		}
	}
	yield* Effect.logInfo(
		`Passed: packed exports and one Effect installation (${withPeers ? 'with' : 'without'} optional peers)`,
	)
})

const checkBundle = Effect.fn('packaging.checkBundle')(function* (consumer: string, entry: string) {
	const fs = yield* FileSystem.FileSystem
	const path = yield* Path.Path
	const meta = yield* Schema.decodeEffect(Schema.fromJsonString(Metafile))(
		yield* fs.readFileString(path.join(consumer, `${entry}.meta.json`)),
	)
	const inputs = Object.keys(meta.inputs)
	const emitted = new Set(
		Object.values(meta.outputs).flatMap((output) =>
			Object.entries(output.inputs)
				.filter(([, input]) => input.bytesInOutput > 0)
				.map(([name]) => name),
		),
	)
	yield* expect(inputs.length > 10, 'real browser bundle', `${entry} has too few inputs`)
	for (const input of inputs) {
		const normalized = `/${input.replaceAll('\\', '/')}`
		yield* expect(
			!emitted.has(input) || !forbiddenBrowserInput.test(normalized),
			'browser emitted input isolation',
			`${entry}: ${input}`,
		)
		yield* expect(
			!entry.endsWith('outbound') ||
				!emitted.has(input) ||
				!normalized.includes('/@humanlayer/channels-delivery/'),
			'outbound contains no delivery engine or storage',
			input,
		)
		yield* expect(
			!entry.startsWith('browser-github') ||
				!emitted.has(input) ||
				!normalized.includes('/@humanlayer/channels-slack/'),
			'GitHub has no Slack runtime',
			input,
		)
		yield* expect(
			entry !== 'browser-github-outbound' ||
				!emitted.has(input) ||
				!normalized.endsWith('/GitHubSubscriptions.js'),
			'GitHub outbound contains no subscription runtime',
			input,
		)
		yield* expect(
			!normalized.includes('/node_modules/@humanlayer/channels-') || normalized.includes('/dist/'),
			'browser built entry',
			input,
		)
		const real = yield* fs.realPath(path.resolve(consumer, input))
		yield* expect(real.startsWith(`${consumer}${path.sep}`), 'browser resolution isolation', entry)
	}
	for (const file of [...Object.values(meta.inputs), ...Object.values(meta.outputs)]) {
		for (const dependency of file.imports) {
			yield* expect(dependency.external !== true, 'no browser externals', `${entry}: ${dependency.path}`)
		}
	}
	const runtimes = new Set(
		inputs.filter((input) => input.includes('/effect/')).map((input) => input.split('/effect/')[0]),
	)
	yield* expect(runtimes.size === 1, 'one bundled Effect runtime', entry)
	yield* expect(
		inputs.some((input) => input.includes('/@humanlayer/channels-')),
		'real package in bundle',
		entry,
	)
	yield* Effect.logInfo(
		`Passed: ${entry}, ${emitted.size} emitted inputs; no SQL/Redis/Node/Alchemy code or externals (${inputs.length} parsed inputs)`,
	)
})

const verify = Effect.gen(function* () {
	const fs = yield* FileSystem.FileSystem
	const path = yield* Path.Path
	const root = yield* fs.realPath(yield* path.fromFileUrl(new URL('../', import.meta.url)))
	const fixtureDirectory = path.join(root, 'tools', 'packaging')
	const searchPath = yield* Config.string('PATH')
	const originalHome = yield* Config.string('HOME')
	const temporary = yield* fs.realPath(yield* fs.makeTempDirectoryScoped({ prefix: 'channels-package-exports-' }))
	yield* checkAncestors(temporary)
	const consumer = path.join(temporary, 'consumer')
	const home = path.join(temporary, 'home')
	const packs = path.join(temporary, 'packs')
	for (const directory of [consumer, home, packs]) yield* fs.makeDirectory(directory)
	const info = yield* Schema.decodeEffect(Schema.fromJsonString(NodeInfo))(
		yield* command({
			check: 'Node toolchain (inspection, 30s)',
			executable: 'node',
			cwd: root,
			args: [
				'--input-type=module',
				'--eval',
				'import process from "node:process"; process.stdout.write(JSON.stringify({ executable: process.execPath, platform: process.platform, arch: process.arch }))',
			],
			env: { PATH: searchPath, HOME: originalHome },
			seconds: 30,
		}),
	)
	const npmCli = yield* fs.realPath(path.join(path.dirname(info.executable), 'npm'))
	const env = {
		PATH: `${path.dirname(info.executable)}${info.platform === 'win32' ? ';' : ':'}${searchPath}`,
		HOME: home,
		TMPDIR: temporary,
		TMP: temporary,
		TEMP: temporary,
		NPM_CONFIG_USERCONFIG: path.join(home, 'user.npmrc'),
		NPM_CONFIG_GLOBALCONFIG: path.join(home, 'global.npmrc'),
		NPM_CONFIG_CACHE: path.join(temporary, 'npm-cache'),
		NO_COLOR: '1',
		CI: '1',
	}
	for (const name of ['user.npmrc', 'global.npmrc']) yield* fs.writeFileString(path.join(home, name), '')
	const run = (
		check: string,
		executable: string,
		args: ReadonlyArray<string>,
		cwd = consumer,
		seconds: 30 | 90 = 30,
	) => command({ check, executable, args, cwd, env, seconds })
	const node = (check: string, args: ReadonlyArray<string>) =>
		run(check, info.executable, ['--no-global-search-paths', ...args])
	const rootManifest = yield* readManifest(path.join(root, 'package.json'))
	const compiler = yield* readManifest(path.join(root, 'node_modules', 'typescript', 'package.json'))
	const compilerVersion = compiler.version
	if (compilerVersion === undefined)
		return yield* new PackagingFailure({ check: 'TypeScript version', detail: 'Installed compiler has no version' })
	const tooling = new Map([
		['typescript', compilerVersion],
		['esbuild-wasm', '0.25.12'],
	])
	const nativeCompiler = `@typescript/typescript-${info.platform}-${info.arch}`
	const nativeVersion = compiler.optionalDependencies?.[nativeCompiler]
	if (nativeVersion !== undefined) tooling.set(nativeCompiler, nativeVersion)
	const packages: Array<Manifest> = []
	const dependencies: Record<string, string> = {}
	const optionalPeers: Record<string, string> = {}
	for (const directory of packageDirectories) {
		const packageDirectory = path.join(root, 'packages', directory)
		const manifest = yield* readManifest(path.join(packageDirectory, 'package.json'))
		packages.push(manifest)
		const tarball = path.join(packs, `${directory}.tgz`)
		yield* fs.remove(path.join(packageDirectory, 'dist'), { recursive: true, force: true })
		yield* run(`build ${directory} (90s)`, 'bun', ['--no-env-file', 'run', 'build'], packageDirectory, 90)
		yield* run(
			`pack ${directory} (30s)`,
			'bun',
			['--no-env-file', 'pm', 'pack', '--ignore-scripts', '--filename', tarball, '--quiet'],
			packageDirectory,
		)
		dependencies[manifest.name] = `file:${tarball}`
		for (const [peer, version] of Object.entries(manifest.peerDependencies ?? {})) {
			const target = manifest.peerDependenciesMeta?.[peer]?.optional === true ? optionalPeers : dependencies
			yield* expect(target[peer] === undefined || target[peer] === version, 'compatible peer versions', peer)
			target[peer] = version
		}
	}
	const effectVersion = rootManifest.catalog?.effect
	if (effectVersion === undefined)
		return yield* new PackagingFailure({ check: 'Effect pin', detail: 'Missing catalog.effect' })
	yield* expect(
		dependencies.effect === effectVersion,
		'required Effect peer',
		'Both packages must declare the pinned Effect runtime',
	)
	const writeConsumerManifest = (withPeers: boolean) =>
		Schema.encodeEffect(Schema.fromJsonString(ConsumerManifest))({
			name: 'channels-packaging-consumer',
			private: true,
			type: 'module',
			dependencies: withPeers ? { ...dependencies, ...optionalPeers } : dependencies,
			devDependencies: Object.fromEntries(tooling),
		}).pipe(Effect.flatMap((json) => fs.writeFileString(path.join(consumer, 'package.json'), json)))
	const install = (check: string) =>
		run(
			check,
			info.executable,
			[
				npmCli,
				'install',
				'--ignore-scripts',
				'--omit=optional',
				'--no-audit',
				'--no-fund',
				'--registry=https://registry.npmjs.org',
				'--fetch-retries=0',
				'--fetch-timeout=20000',
			],
			consumer,
			90,
		)
	for (const file of [
		'consumer.ts',
		'organizations.ts',
		'backends.ts',
		'guard.mjs',
		'runtime.mjs',
		'backends.mjs',
		'remote.mjs',
		...browserEntries.map((entry) => `${entry}.ts`),
	]) {
		const source = file.endsWith('.ts') ? `${file}.fixture` : file
		yield* fs.copyFile(path.join(fixtureDirectory, source), path.join(consumer, file))
	}
	const config = yield* fs.readFileString(path.join(fixtureDirectory, 'tsconfig.consumer.json'))
	yield* fs.writeFileString(path.join(consumer, 'tsconfig.json'), config)
	yield* fs.copyFile(
		path.join(fixtureDirectory, 'tsconfig.backends.json'),
		path.join(consumer, 'tsconfig.backends.json'),
	)
	yield* writeConsumerManifest(false)
	yield* install('install required dependencies and isolated tooling, no optional peers (90s)')
	yield* checkInstalled(consumer, packages, effectVersion, false)
	yield* node('NodeNext declaration consumer, skipLibCheck=false (30s)', [
		'node_modules/typescript/bin/tsc',
		'-p',
		'tsconfig.json',
	])
	yield* node('delivery contract/client imports are inert and implementation-free (30s)', [
		'--import',
		'./guard.mjs',
		'./remote.mjs',
	])
	yield* node('Node ESM, memory service identity across entries and Slack, no network (30s)', [
		'--import',
		'./guard.mjs',
		'./runtime.mjs',
	])
	for (const entry of browserEntries) {
		yield* node(`real browser bundle ${entry} (30s)`, [
			'node_modules/esbuild-wasm/bin/esbuild',
			`${entry}.ts`,
			'--bundle',
			'--platform=browser',
			'--format=esm',
			`--outfile=${entry}.js`,
			`--metafile=${entry}.meta.json`,
			'--log-level=warning',
		])
		yield* checkBundle(consumer, entry)
	}
	yield* node('bundler-resolution declaration consumer, skipLibCheck=false (30s)', [
		'node_modules/typescript/bin/tsc',
		'-p',
		'tsconfig.json',
		'--module',
		'ESNext',
		'--moduleResolution',
		'bundler',
		'--noEmit',
	])
	yield* writeConsumerManifest(true)
	yield* install('explicit optional peer consumer (90s)')
	yield* checkInstalled(consumer, packages, effectVersion, true)
	yield* node('backend/client import only: no Layer acquisition, network, or application env (30s)', [
		'--import',
		'./guard.mjs',
		'./backends.mjs',
	])
	yield* node('backend/client declarations, skipLibCheck=false (30s)', [
		'node_modules/typescript/bin/tsc',
		'-p',
		'tsconfig.backends.json',
	])
	yield* node('cross-entry identity with peers installed (30s)', ['--import', './guard.mjs', './runtime.mjs'])
	yield* Effect.logInfo(
		'PASS: actual packed packages, isolated dependency graphs, Node ESM, strict declarations, browser bundles, service identity, inert backend/client imports, one Effect runtime',
	)
})

verify.pipe(Effect.timeout('115 seconds'), Effect.scoped, Effect.provide(NodeServices.layer), NodeRuntime.runMain)
