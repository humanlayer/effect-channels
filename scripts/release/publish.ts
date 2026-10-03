/**
 * Publishes the staged packages in `.release/`, one at a time, in dependency order.
 *
 * A version already on npm is skipped, so a release that failed halfway can be run again. In GitHub
 * Actions npm signs in through trusted publishing; from a laptop it uses your `npm login`.
 */
import { join } from 'node:path'
import { parseArgs } from 'node:util'

import { libraries, stage } from './manifest'

const values = parseArgs({
	options: {
		version: { type: 'string' },
		tag: { type: 'string', default: 'latest' },
		'dry-run': { type: 'boolean', default: false },
	},
}).values
if (values.version === undefined) throw new Error('--version is required')
const dryRun = values['dry-run']

const alreadyPublished = async (name: string, version: string) => {
	const child = Bun.spawn(['npm', 'view', `${name}@${version}`, 'version', '--json'], {
		stdout: 'pipe',
		stderr: 'ignore',
	})
	const output = await new Response(child.stdout).text()
	return (await child.exited) === 0 && output.trim().length > 0
}

for (const directory of libraries) {
	const path = join(stage, 'packages', directory)
	const manifest: { name: string; version: string } = await Bun.file(join(path, 'package.json')).json()
	if (manifest.version !== values.version)
		throw new Error(`${manifest.name} is staged at ${manifest.version}, expected ${values.version}`)
	if (!dryRun && (await alreadyPublished(manifest.name, manifest.version))) {
		console.log(`skipping ${manifest.name}@${manifest.version}: already published`)
		continue
	}
	console.log(`${dryRun ? 'dry run of' : 'publishing'} ${manifest.name}@${manifest.version} as ${values.tag}`)
	const child = Bun.spawn(
		['npm', 'publish', '--access', 'public', '--tag', values.tag, ...(dryRun ? ['--dry-run'] : [])],
		{ cwd: path, stdin: 'inherit', stdout: 'inherit', stderr: 'inherit' },
	)
	if ((await child.exited) !== 0) throw new Error(`Publish failed for ${manifest.name}`)
}
