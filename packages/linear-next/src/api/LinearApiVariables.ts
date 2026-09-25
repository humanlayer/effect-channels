import type { LinearListAppUsersRequest, LinearListAssignableUsersRequest } from '../LinearApi'

export const linearUserPageVariables = (input: LinearListAppUsersRequest | LinearListAssignableUsersRequest) => {
	const variables: { first: number; after?: string; query?: string } = { first: input.first ?? 50 }
	if (input.after !== undefined) variables.after = input.after
	if (input.query !== undefined) variables.query = input.query
	return variables
}
