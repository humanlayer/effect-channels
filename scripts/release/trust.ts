/**
 * Connects every package to GitHub Actions trusted publishing, so `release.yml` can publish without an
 * npm token. Run it once, from a laptop signed in with `npm login`, after each package exists on npm.
 *
 * A package that already trusts `release.yml` in this repository is skipped, so it is safe to run again.
 * Pass `--dry-run` to see what it would do.
 */
import { join } from 'node:path'
import { parseArgs } from 'node:util'

import { json, libraries, root } from './manifest'

const repository = 'humanlayer/effect-channels'
const workflow = 'release.yml'
const dryRun = parseArgs({ options: { 'dry-run': { type: 'boolean', default: false } } }).values['dry-run']

const npm = async (args: Array<string>, capture: boolean) => {
	const child = Bun.spawn(['npm', ...args], {
		stdin: 'inherit',
		stdout: capture ? 'pipe' : 'inherit',
		stderr: 'inherit',
	})
	const output = capture ? await new Response(child.stdout).text() : ''
	return { exitCode: await child.exited, output }
}

for (const directory of libraries) {
	const { name } = await json<{ name: string }>(join(root, 'packages', directory, 'package.json'))
	const existing = await npm(['trust', 'list', name, '--json'], true)
	if (existing.exitCode !== 0) throw new Error(`Could not list trusted publishers for ${name}; is it on npm yet?`)
	if (existing.output.includes(repository) && existing.output.includes(workflow)) {
		console.log(`skipping ${name}: already trusts ${repository} ${workflow}`)
		continue
	}
	console.log(`${dryRun ? 'dry run of' : 'connecting'} ${name} to ${repository} ${workflow}`)
	const args = ['trust', 'github', name, '--file', workflow, '--repo', repository, '--allow-publish', '--yes']
	const { exitCode } = await npm(dryRun ? [...args, '--dry-run'] : args, false)
	if (exitCode !== 0) throw new Error(`npm trust failed for ${name}`)
}
