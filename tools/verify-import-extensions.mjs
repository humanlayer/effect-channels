import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('../', import.meta.url))
const temporary = mkdtempSync(join(tmpdir(), 'channels-imports-'))
const rule = 'import-extensions/no-typescript-specifiers'
const run = (binary, args) => {
	const result = spawnSync(resolve(root, 'node_modules/.bin', binary), args, { cwd: root, encoding: 'utf8' })
	assert.ifError(result.error)
	return { status: result.status, output: result.stdout + result.stderr }
}
const write = (name, contents) => {
	const path = join(temporary, name)
	writeFileSync(path, contents)
	return path
}

const checkConfigs = (directory) => {
	for (const entry of readdirSync(directory, { withFileTypes: true })) {
		if (['node_modules', 'dist', 'archive'].includes(entry.name) || entry.name.startsWith('.')) continue
		const path = join(directory, entry.name)
		if (entry.isDirectory()) checkConfigs(path)
		else if (/^tsconfig.*\.json$/u.test(entry.name)) {
			const options = JSON.parse(readFileSync(path, 'utf8')).compilerOptions
			for (const option of ['allowImportingTsExtensions', 'rewriteRelativeImportExtensions']) {
				assert.notEqual(options?.[option], true, `${path}: ${option} bypasses the source-extension policy`)
			}
		}
	}
}

try {
	const config = write(
		'oxlint.json',
		JSON.stringify({
			categories: { correctness: 'off' },
			jsPlugins: [{ name: 'import-extensions', specifier: resolve(root, 'tools/oxlint/import-extensions.mjs') }],
			rules: { [rule]: 'error' },
			ignorePatterns: ['**/node_modules/**', '**/dist/**'],
		}),
	)
	const lint = (path) => run('oxlint', ['-c', config, path])
	for (const extension of ['ts', 'tsx', 'mts', 'cts']) {
		for (const source of [
			`import { value } from './target.${extension}'`,
			`import type { Value } from './target.${extension}'`,
			`import './target.${extension}'`,
			`export { value } from './target.${extension}'`,
			`export type { Value } from './target.${extension}'`,
			`export * from './target.${extension}'`,
			`void import('./target.${extension}')`,
			`void import(\`./target.${extension}\`)`,
			`void import(\`./\${name}.${extension}\`)`,
			`type Value = import('./target.${extension}').Value`,
			`import value = require('./target.${extension}')`,
		]) {
			const result = lint(write('negative.ts', source))
			assert.equal(result.status, 1, `${source}\n${result.output}`)
			assert.match(result.output, /no-typescript-specifiers/)
		}
	}
	for (const extension of ['js', 'mjs', 'cjs', '']) {
		const suffix = extension ? `.${extension}` : ''
		const result = lint(
			write(
				'positive.ts',
				`import { value } from './target${suffix}'; export * from './target${suffix}'; void import('./target${suffix}'); const entry = './target.ts'; new URL('./target.ts', import.meta.url)`,
			),
		)
		assert.equal(result.status, 0, result.output)
	}
	write('target.ts', 'export const value = 1')
	write('compiler.ts', "import { value } from './target.ts'; export { value }")
	const compilerResults = []
	for (const base of [
		'tsconfig.json',
		...readdirSync(resolve(root, 'packages')).map((name) => `packages/${name}/tsconfig.build.json`),
	]) {
		const project = write(
			'tsconfig.json',
			JSON.stringify({
				extends: resolve(root, base),
				compilerOptions: { types: [], rootDir: temporary, outDir: join(temporary, 'dist') },
				include: [],
				files: ['./compiler.ts'],
			}),
		)
		compilerResults.push({ base, result: run('tsc', ['-p', project]) })
	}
	const project = write(
		'tsconfig.json',
		JSON.stringify({
			extends: resolve(root, 'tsconfig.json'),
			compilerOptions: { types: [], allowImportingTsExtensions: false, rewriteRelativeImportExtensions: true },
			include: [],
			files: ['./compiler.ts'],
		}),
	)
	assert.equal(
		run('tsc', ['-p', project]).status,
		0,
		'Demonstrate the rewriteRelativeImportExtensions compiler loophole',
	)
	assert.equal(
		lint(join(temporary, 'compiler.ts')).status,
		1,
		'Lint must reject even when rewriting permits the import',
	)
	process.stdout.write('44 negative lint cases, positive cases and rewrite loophole probes passed.\n')
	for (const { base, result } of compilerResults) {
		assert.notEqual(result.status, 0, `${base} must reject source extensions`)
		assert.match(result.output, /TS5097/, result.output)
	}
	checkConfigs(root)
	for (const path of ['packages', 'examples', 'tools', 'vite.config.ts']) {
		const result = lint(resolve(root, path))
		assert.equal(result.status, 0, result.output)
	}
	process.stdout.write('Import policy: compiler rejection, config enforcement and repository scan passed.\n')
} finally {
	rmSync(temporary, { recursive: true, force: true })
}
