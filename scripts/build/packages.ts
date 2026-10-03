/**
 * Builds every package in `packages/` into its own `dist/`: one ESM file per export, and declaration
 * files that resolve under both `bundler` and `NodeNext` module resolution.
 */
import { readdir, rm, stat } from 'node:fs/promises'
import { dirname, join } from 'node:path'

import { json, libraries, root } from '../release/manifest'

const exists = (path: string) =>
	stat(path).then(
		() => true,
		() => false,
	)

/**
 * The source keeps relative imports without extensions, which `NodeNext` refuses in declaration files.
 * Point each one at its emitted `.js` (or `/index.js`); TypeScript reads the `.d.ts` beside it.
 */
const relativeSpecifier = /((?:from|import)\s*\(?\s*['"])(\.{1,2}\/[^'"]*?)(['"])/g

const rewriteDeclarations = async (directory: string) => {
	for (const entry of await readdir(directory, { recursive: true })) {
		if (!entry.endsWith('.d.ts')) continue
		const file = join(directory, entry)
		const text = await Bun.file(file).text()
		const replacements = new Map<string, string>()
		for (const [, , specifier] of text.matchAll(relativeSpecifier)) {
			if (specifier === undefined || /\.[cm]?js$/.test(specifier) || replacements.has(specifier)) continue
			const target = join(dirname(file), specifier)
			if (await exists(`${target}.d.ts`)) replacements.set(specifier, `${specifier}.js`)
			else if (await exists(join(target, 'index.d.ts'))) replacements.set(specifier, `${specifier}/index.js`)
			else throw new Error(`${file} imports ${specifier}, which has no declaration file`)
		}
		await Bun.write(
			file,
			text.replace(relativeSpecifier, (match, before: string, specifier: string, after: string) => {
				const replacement = replacements.get(specifier)
				return replacement === undefined ? match : `${before}${replacement}${after}`
			}),
		)
	}
}

for (const name of libraries) {
	const directory = join(root, 'packages', name)
	const manifest = await json<{ name: string; exports: Record<string, string> }>(join(directory, 'package.json'))
	const entrypoints = Object.values(manifest.exports).filter((entry) => /^\.\/src\/.*\.ts$/.test(entry))
	if (entrypoints.length === 0) throw new Error(`${manifest.name} exports no TypeScript entrypoint`)
	const outdir = join(directory, 'dist')
	await rm(outdir, { recursive: true, force: true })

	const result = await Bun.build({
		entrypoints: entrypoints.map((entry) => join(directory, entry)),
		outdir,
		root: join(directory, 'src'),
		target: 'browser',
		format: 'esm',
		packages: 'external',
		sourcemap: 'external',
	})
	if (!result.success) throw new AggregateError(result.logs, `Failed to build ${manifest.name}`)

	const config = join(directory, 'tsconfig.release.json')
	await Bun.write(
		config,
		`${JSON.stringify(
			{
				extends: './tsconfig.json',
				compilerOptions: {
					noEmit: false,
					declaration: true,
					emitDeclarationOnly: true,
					outDir: './dist',
					rootDir: './src',
				},
				include: ['src'],
			},
			null,
			2,
		)}\n`,
	)
	const declarations = Bun.spawn(['bunx', 'tsc', '-p', config], { cwd: root, stdout: 'inherit', stderr: 'inherit' })
	const exitCode = await declarations.exited
	await rm(config, { force: true })
	if (exitCode !== 0) throw new Error(`Failed to emit declarations for ${manifest.name}`)
	await rewriteDeclarations(outdir)
	console.log(`built ${manifest.name}`)
}
