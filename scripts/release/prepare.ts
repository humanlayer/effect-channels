/**
 * Copies every built package into `.release/packages/<name>` with a publishable `package.json`:
 * the release version, `catalog:` and `workspace:` ranges replaced by real ones, and exports
 * pointing at `dist/` instead of `src/`.
 */
import { cp, mkdir, readdir, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

import { json, libraries, repository, root, scope, stage, versionPattern } from './manifest'

const version = parseArgs({ options: { version: { type: 'string' } } }).values.version
if (version === undefined || !versionPattern.test(version)) throw new Error('A valid --version is required')

type DependencyMap = Record<string, string>
type ExportTarget = { types: string; import: string; default: string }
type PackageManifest = {
	[key: string]: unknown
	name: string
	exports: Record<string, string | ExportTarget>
	dependencies?: DependencyMap
	peerDependencies?: DependencyMap
}

const directories = (await readdir(join(root, 'packages'), { withFileTypes: true }))
	.filter((entry) => entry.isDirectory())
	.map((entry) => entry.name)
	.sort()
if (JSON.stringify(directories) !== JSON.stringify([...libraries].sort()))
	throw new Error(
		`scripts/release/manifest.ts lists ${libraries.join(', ')}, but packages/ has ${directories.join(', ')}`,
	)

const { catalog } = await json<{ catalog: Record<string, string> }>(join(root, 'package.json'))
const fromCatalog = (name: string) => {
	const range = catalog[name]
	if (range === undefined) throw new Error(`Missing catalog entry ${name}`)
	return range
}

/**
 * Our own packages are pinned to this release. Peers such as `effect` accept any compatible version,
 * so an application on a later patch can install them without a conflict.
 */
const resolveRanges = async (dependencies: DependencyMap | undefined, peer: boolean, source: string) => {
	if (dependencies === undefined) return
	for (const [name, range] of Object.entries(dependencies)) {
		if (range === 'catalog:') dependencies[name] = peer ? await peerRange(name, source) : fromCatalog(name)
		else if (range.startsWith('workspace:')) dependencies[name] = version
	}
}

/**
 * A peer's range: `^` and its catalog version. A catalog entry that is not a version, such as a preview
 * build's URL, becomes `^` and the version the package was built against, which npm can match.
 */
const peerRange = async (name: string, source: string) => {
	const spec = fromCatalog(name)
	if (/^\d/.test(spec)) return `^${spec}`
	const installed = await json<{ version: string }>(join(source, 'node_modules', name, 'package.json'))
	return `^${installed.version}`
}

const toDist = (source: string) => source.replace(/^\.\/src\//, './dist/').replace(/\.ts$/, '.js')
const toTypes = (source: string) => toDist(source).replace(/\.js$/, '.d.ts')

await rm(stage, { recursive: true, force: true })

for (const directory of libraries) {
	const source = join(root, 'packages', directory)
	const dest = join(stage, 'packages', directory)
	const manifest = structuredClone(await json<PackageManifest>(join(source, 'package.json')))
	if (!manifest.name.startsWith(`${scope}/`)) throw new Error(`${manifest.name} is outside the ${scope} scope`)

	manifest.version = version
	manifest.private = false
	manifest.license = 'MIT'
	manifest.publishConfig = { access: 'public' }
	manifest.repository = { ...repository, directory: `packages/${directory}` }
	manifest.homepage = 'https://github.com/humanlayer/effect-channels#readme'
	manifest.bugs = { url: 'https://github.com/humanlayer/effect-channels/issues' }
	manifest.files = ['dist', 'README.md', 'LICENSE']
	delete manifest.devDependencies
	delete manifest.scripts
	await resolveRanges(manifest.dependencies, false, source)
	await resolveRanges(manifest.peerDependencies, true, source)

	const sources = Object.entries(manifest.exports).map(([key, value]) => {
		if (typeof value !== 'string') throw new Error(`${manifest.name} export ${key} is not a source path`)
		return [key, value] as const
	})
	const main = sources.find(([key]) => key === '.')?.[1]
	if (main === undefined) throw new Error(`${manifest.name} has no root export`)
	manifest.exports = Object.fromEntries(
		sources.map(([key, value]) => [key, { types: toTypes(value), import: toDist(value), default: toDist(value) }]),
	)
	manifest.module = toDist(main)
	manifest.types = toTypes(main)

	await mkdir(dest, { recursive: true })
	await cp(join(source, 'dist'), join(dest, 'dist'), { recursive: true })
	if (await Bun.file(join(source, 'README.md')).exists()) await cp(join(source, 'README.md'), join(dest, 'README.md'))
	await cp(join(root, 'LICENSE'), join(dest, 'LICENSE'))
	await Bun.write(join(dest, 'package.json'), `${JSON.stringify(manifest, null, 2)}\n`)
	console.log(`staged ${manifest.name}@${version}`)
}
