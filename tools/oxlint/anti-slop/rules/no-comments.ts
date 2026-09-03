import { defineRule } from "@oxlint/plugins";

/** Reject implementation comments except public JSDoc and SAFETY justifications. */
export const noCommentsRule = defineRule({
	meta: {
		type: "problem",
		docs: {
			description:
				"Reject implementation comments; public JSDoc and SAFETY justifications remain allowed.",
		},
		messages: {
			comment:
				"Remove this comment and make the code communicate its intent directly.",
		},
	},
	createOnce(context) {
		return {
			Program() {
				for (const comment of context.sourceCode.getAllComments()) {
					if (comment.type === "Block" && comment.value.startsWith("*")) continue;
					if (/\bSAFETY\s*:/u.test(comment.value)) continue;
					context.report({ node: comment, messageId: "comment" });
				}
			},
		};
	},
});
