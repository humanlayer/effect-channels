import * as Alchemy from 'alchemy'
import * as Cloudflare from 'alchemy/Cloudflare'
import * as Effect from 'effect/Effect'

import { ComputerWorker } from './src/computer/ComputerWorker'
import IngressWorker from './src/Worker'

export default Alchemy.Stack(
	'AlchemyCloudflareExample',
	{
		providers: Cloudflare.providers(),
		state: Cloudflare.state(),
	},
	Effect.gen(function* () {
		yield* ComputerWorker
		const worker = yield* IngressWorker

		return {
			url: worker.url,
		}
	}),
)
