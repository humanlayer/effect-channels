import { defineRule, eslintCompatPlugin } from '@oxlint/plugins'

const sourceExtension = /\.(?:tsx?|mts|cts)(?:[?#].*)?$/u

export default eslintCompatPlugin({
	meta: { name: 'import-extensions' },
	rules: {
		'no-typescript-specifiers': defineRule({
			meta: {
				type: 'problem',
				schema: [],
				messages: {
					extension:
						'Use emitted .js/.mjs/.cjs extensions (or extensionless imports), not TypeScript source extensions.',
				},
			},
			createOnce(context) {
				function check(node) {
					if (!node) return
					const value = node.type === 'TemplateLiteral' ? node.quasis.at(-1).value.cooked : node.value
					if (value != null && sourceExtension.test(value)) {
						context.report({ node, messageId: 'extension' })
					}
				}
				return {
					ImportDeclaration(node) {
						check(node.source)
					},
					ExportNamedDeclaration(node) {
						check(node.source)
					},
					ExportAllDeclaration(node) {
						check(node.source)
					},
					ImportExpression(node) {
						check(node.source)
					},
					TSImportType(node) {
						check(node.source)
					},
					TSExternalModuleReference(node) {
						check(node.expression)
					},
				}
			},
		}),
	},
})
