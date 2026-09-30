/**
 * This file defines external links: labeled `https` links to something outside the conversation,
 * such as the remote job's page or a pull request it opened.
 *
 * Code sets a link, never the agent's Markdown. The callback passes links to `handoff`, and a remote
 * worker adds later ones through the delivery API. Each new link is saved with an `AddExternalLink`
 * output operation; a repeat of the same URL is a replay.
 */
import { Schema } from 'effect'

/** An `https` URL. */
export const HttpsUrl = Schema.NonEmptyString.check(Schema.isMaxLength(2_048), Schema.isPattern(/^https:\/\/\S+$/))

/** A labeled link to something outside the conversation, such as the remote job's page. */
export const ExternalLink = Schema.TaggedStruct('ExternalLink', {
	label: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
	url: HttpsUrl,
})
export type ExternalLink = typeof ExternalLink.Type

/** Show a link where the provider can. Slack, GitHub, and Linear issues settle it as a no-op for now. */
export const AddExternalLink = Schema.TaggedStruct('AddExternalLink', {
	link: ExternalLink,
})
export type AddExternalLink = typeof AddExternalLink.Type
