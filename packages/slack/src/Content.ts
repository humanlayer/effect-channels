import { Schema } from 'effect'

import { FileUpload } from './Model.ts'

export const ActionButton = Schema.TaggedStruct('ActionButton', {
	id: Schema.NonEmptyString,
	label: Schema.NonEmptyString,
	style: Schema.optionalKey(Schema.Literals(['default', 'primary', 'danger'])),
})
export type ActionButton = typeof ActionButton.Type

export const LinkButton = Schema.TaggedStruct('LinkButton', {
	label: Schema.NonEmptyString,
	url: Schema.URLFromString,
})
export type LinkButton = typeof LinkButton.Type

export const ContentAction = Schema.Union([ActionButton, LinkButton])
export type ContentAction = typeof ContentAction.Type

export const PlainTextContent = Schema.TaggedStruct('PlainTextContent', {
	text: Schema.String,
	files: Schema.optionalKey(Schema.Array(FileUpload)),
})
export type PlainTextContent = typeof PlainTextContent.Type

export const MarkdownContent = Schema.TaggedStruct('MarkdownContent', {
	markdown: Schema.String,
	actions: Schema.optionalKey(Schema.Array(ContentAction)),
	files: Schema.optionalKey(Schema.Array(FileUpload)),
})
export type MarkdownContent = typeof MarkdownContent.Type

export const TextContentNode = Schema.TaggedStruct('TextContentNode', {
	text: Schema.String,
	marks: Schema.Array(Schema.Literals(['strong', 'emphasis', 'strikethrough', 'code'])),
})
export type TextContentNode = typeof TextContentNode.Type

export const LinkContentNode = Schema.TaggedStruct('LinkContentNode', {
	label: Schema.String,
	url: Schema.URLFromString,
})
export type LinkContentNode = typeof LinkContentNode.Type

export const InlineContentNode = Schema.Union([TextContentNode, LinkContentNode])
export type InlineContentNode = typeof InlineContentNode.Type

export const ParagraphContentBlock = Schema.TaggedStruct('ParagraphContentBlock', {
	children: Schema.Array(InlineContentNode),
})
export type ParagraphContentBlock = typeof ParagraphContentBlock.Type

export const CodeContentBlock = Schema.TaggedStruct('CodeContentBlock', {
	code: Schema.String,
	language: Schema.optionalKey(Schema.String),
})
export type CodeContentBlock = typeof CodeContentBlock.Type

export const ContentBlock = Schema.Union([ParagraphContentBlock, CodeContentBlock])
export type ContentBlock = typeof ContentBlock.Type

export const StructuredContent = Schema.TaggedStruct('StructuredContent', {
	blocks: Schema.Array(ContentBlock),
	actions: Schema.optionalKey(Schema.Array(ContentAction)),
	files: Schema.optionalKey(Schema.Array(FileUpload)),
})
export type StructuredContent = typeof StructuredContent.Type

export const Content = Schema.Union([PlainTextContent, MarkdownContent, StructuredContent])
export type Content = typeof Content.Type
export const SlackContent = Content
