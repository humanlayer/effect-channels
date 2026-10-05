/**
 * Builds every package in `packages/` into its own `dist/` with TypeScript: one `.js`, `.js.map`, and
 * `.d.ts` per source file, which Node.js and `NodeNext` projects can both read.
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
 * The source keeps relative imports without extensions, which Node.js refuses in ESM and `NodeNext`
 * refuses in declaration files. Point each one at its emitted `.js` (or `/index.js`).
 */
const relativeSpecifier = /((?:from|import)\s*\(?\s*['"])(\.{1,2}\/[^'"]*?)(['"])/g

const addExtensions = async (directory: string) => {
	for (const entry of await readdir(directory, { recursive: true })) {
		if (!entry.endsWith('.js') && !entry.endsWith('.d.ts')) continue
		const file = join(directory, entry)
		const text = await Bun.file(file).text()
		const replacements = new Map<string, string>()
		for (const [, , specifier] of text.matchAll(relativeSpecifier)) {
			if (specifier === undefined || /\.[cm]?js$/.test(specifier) || replacements.has(specifier)) continue
			const target = join(dirname(file), specifier)
			if (await exists(`${target}.js`)) replacements.set(specifier, `${specifier}.js`)
			else if (await exists(join(target, 'index.js'))) replacements.set(specifier, `${specifier}/index.js`)
			else throw new Error(`${file} imports ${specifier}, which was not emitted`)
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
	if (!Object.values(manifest.exports).every((entry) => /^\.\/src\/.*\.ts$/.test(entry)))
		throw new Error(`${manifest.name} must export TypeScript files in src/`)
	const outdir = join(directory, 'dist')
	await rm(outdir, { recursive: true, force: true })

	const config = join(directory, 'tsconfig.release.json')
	await Bun.write(
		config,
		`${JSON.stringify(
			{
				extends: './tsconfig.json',
				compilerOptions: {
					noEmit: false,
					declaration: true,
					sourceMap: true,
					outDir: './dist',
					rootDir: './src',
				},
				include: ['src'],
			},
			null,
			2,
		)}\n`,
	)
	const compile = Bun.spawn(['bunx', 'tsc', '-p', config], { cwd: root, stdout: 'inherit', stderr: 'inherit' })
	const exitCode = await compile.exited
	await rm(config, { force: true })
	if (exitCode !== 0) throw new Error(`Failed to compile ${manifest.name}`)
	await addExtensions(outdir)
	console.log(`built ${manifest.name}`)
}
