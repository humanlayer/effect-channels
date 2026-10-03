/**
 * Checks the staged packages in `.release/` before anything is published.
 *
 * 1. Each `package.json` is publishable: the release version, public, no `workspace:` or `catalog:`
 *    ranges, Effect and Alchemy only as peers, our own packages pinned to this release, and exports that
 *    point at built files that exist.
 * 2. The packed tarballs work for a real user: npm installs them into a throwaway project, a sample app
 *    typechecks against them with strict Node.js settings, and plain Node.js imports every package.
 */
import { mkdtemp, readdir, rm, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

import { json, libraries, root, scope, stage, versionPattern } from './manifest'

const version = parseArgs({ options: { version: { type: 'string' } } }).values.version
if (version === undefined || !versionPattern.test(version)) throw new Error('A valid --version is required')

type ExportTarget = { types: string; import: string; default: string }
type StagedManifest = {
	name: string
	version: string
	private?: boolean
	publishConfig?: { access?: string }
	dependencies?: Record<string, string>
	peerDependencies?: Record<string, string>
	exports: Record<string, ExportTarget>
}

const peerOnly = (name: string) => name === 'effect' || name.startsWith('@effect/') || name === 'alchemy'

const exists = (path: string) =>
	stat(path).then(
		() => true,
		() => false,
	)

const run = async (command: Array<string>, cwd: string) => {
	const child = Bun.spawn(command, {
		cwd,
		stdout: 'pipe',
		stderr: 'pipe',
		env: {
			...process.env,
			npm_config_update_notifier: 'false',
			npm_config_fund: 'false',
			npm_config_audit: 'false',
		},
	})
	const [stdout, stderr, exitCode] = await Promise.all([
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
		child.exited,
	])
	if (exitCode !== 0) throw new Error(`Command failed in ${cwd}: ${command.join(' ')}\n${stdout}${stderr}`)
	return stdout
}

const peers = new Map<string, string>()

for (const directory of libraries) {
	const path = join(stage, 'packages', directory)
	const manifest = await json<StagedManifest>(join(path, 'package.json'))
	const fail = (message: string): never => {
		throw new Error(`${manifest.name}: ${message}`)
	}
	if (manifest.version !== version) fail(`is ${manifest.version}, expected ${version}`)
	if (manifest.private === true) fail('is private')
	if (manifest.publishConfig?.access !== 'public') fail('is not public')
	if (await exists(join(path, 'src'))) fail('would publish its source files')
	for (const file of ['README.md', 'LICENSE']) if (!(await exists(join(path, file)))) fail(`is missing ${file}`)

	for (const [name, range] of Object.entries(manifest.dependencies ?? {})) {
		if (peerOnly(name)) fail(`must declare ${name} as a peer dependency`)
		if (range.includes('workspace:') || range === 'catalog:') fail(`has unresolved dependency ${name}`)
		if (name.startsWith(`${scope}/`) && range !== version) fail(`does not pin ${name} to ${version}`)
	}
	for (const [name, range] of Object.entries(manifest.peerDependencies ?? {})) {
		if (range.includes('workspace:') || range === 'catalog:') fail(`has unresolved peer ${name}`)
		peers.set(name, range)
	}
	if (manifest.peerDependencies?.effect === undefined) fail('must declare effect as a peer dependency')

	for (const [key, target] of Object.entries(manifest.exports)) {
		for (const file of [target.types, target.import, target.default]) {
			if (file.includes('/src/')) fail(`export ${key} points at source: ${file}`)
			if (!(await exists(join(path, file)))) fail(`export ${key} points at a missing file: ${file}`)
		}
	}
	const files = await readdir(join(path, 'dist'), { recursive: true })
	if (files.some((file) => /(?<!\.d)\.ts$/.test(file))) fail('dist/ contains TypeScript source')
	console.log(`checked ${manifest.name}@${manifest.version}`)
}

const workspace = await mkdtemp(join(tmpdir(), 'channels-release-'))
try {
	const archives = join(workspace, 'archives')
	const consumer = join(workspace, 'consumer')
	await Bun.write(join(archives, '.keep'), '')
	const tarballs = Array<string>()
	for (const directory of libraries) {
		const output = await run(
			['npm', 'pack', '--json', '--pack-destination', archives],
			join(stage, 'packages', directory),
		)
		const [packed] = JSON.parse(output) as Array<{ filename: string }>
		if (packed === undefined) throw new Error(`npm pack produced nothing for ${directory}`)
		tarballs.push(join(archives, packed.filename))
	}

	const fixtures = join(root, 'scripts/release/fixtures')
	await Bun.write(
		join(consumer, 'package.json'),
		`${JSON.stringify({ name: 'channels-release-consumer', private: true, type: 'module' }, null, 2)}\n`,
	)
	await Bun.write(
		join(consumer, 'tsconfig.json'),
		`${JSON.stringify(
			{
				compilerOptions: {
					target: 'ES2023',
					lib: ['ESNext', 'DOM', 'DOM.Iterable'],
					module: 'NodeNext',
					moduleResolution: 'NodeNext',
					strict: true,
					skipLibCheck: false,
					noEmit: true,
					types: [],
				},
				files: ['consumer.ts'],
			},
			null,
			2,
		)}\n`,
	)
	for (const file of ['consumer.ts', 'guard.mjs', 'runtime.mjs'])
		await Bun.write(join(consumer, file), Bun.file(join(fixtures, file)))

	/** Importing `alchemy/Cloudflare` loads `@effect/platform-node`, an optional peer every Alchemy app installs. */
	const alchemyPeers = peers.has('alchemy') ? [`@effect/platform-node@${peers.get('effect')}`] : []
	const peerSpecs = [...[...peers].map(([name, range]) => `${name}@${range}`), ...alchemyPeers]
	await run(['npm', 'install', '--ignore-scripts', '--no-package-lock', ...peerSpecs, ...tarballs], consumer)
	console.log(`installed ${tarballs.length} packages with ${peerSpecs.join(', ')}`)

	/**
	 * `skipLibCheck: false` reads every declaration file, including Alchemy's, which need Cloudflare and
	 * Node.js globals this project does not install. Only errors in the sample app or our packages count.
	 */
	const typecheck = Bun.spawn([join(root, 'node_modules/.bin/tsc'), '-p', 'tsconfig.json'], {
		cwd: consumer,
		stdout: 'pipe',
		stderr: 'pipe',
	})
	const ours = (await new Response(typecheck.stdout).text())
		.split('\n')
		.filter((line) => line.startsWith('consumer.ts') || line.startsWith(`node_modules/${scope}/`))
	await typecheck.exited
	if (ours.length > 0) throw new Error(`The sample app does not typecheck against the packages:\n${ours.join('\n')}`)
	console.log('typechecked the sample app with NodeNext resolution')

	process.stdout.write(await run(['node', 'runtime.mjs'], consumer))
	console.log(`validated ${libraries.length} packages at ${version}`)
} finally {
	await rm(workspace, { recursive: true, force: true })
}
