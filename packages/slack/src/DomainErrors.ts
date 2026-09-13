import { Cause, Option, Schema } from 'effect'

import { OrgId, ProviderName, TenantId, ThreadId, UserId } from './Model.js'

export const Retryability = Schema.Literals(['retryable', 'non_retryable'])
export type Retryability = typeof Retryability.Type

export const RetryabilityMetadata = Schema.Struct({ retryability: Retryability })
export type RetryabilityMetadata = typeof RetryabilityMetadata.Type

/** Returns retryability metadata from a parsed retryability carrier. */
export const retryabilityOf = (error: RetryabilityMetadata): Retryability => error.retryability

/** Returns whether a typed failure is explicitly non-retryable. */
export const isNonRetryableError = (error: RetryabilityMetadata): boolean => retryabilityOf(error) === 'non_retryable'

/** Returns whether an Effect cause contains an explicitly non-retryable typed failure. */
export const isNonRetryableCause = (cause: Cause.Cause<unknown>): boolean =>
	Option.match(Cause.findErrorOption(cause), {
		onNone: () => false,
		onSome: (error) => Schema.is(RetryabilityMetadata)(error) && isNonRetryableError(error),
	})

export class UnknownProvider extends Schema.TaggedError<UnknownProvider>()('UnknownProvider', {
	provider: Schema.String,
	retryability: Schema.optionalKey(Retryability),
}) {}

export class UnknownTenant extends Schema.TaggedError<UnknownTenant>()('UnknownTenant', {
	provider: ProviderName,
	tenant: TenantId,
	retryability: Schema.optionalKey(Retryability),
}) {}

export class TenantDisabled extends Schema.TaggedError<TenantDisabled>()('TenantDisabled', {
	orgId: OrgId,
	provider: ProviderName,
	tenant: TenantId,
	retryability: Schema.optionalKey(Retryability),
}) {}

export class PostFailed extends Schema.TaggedError<PostFailed>()('PostFailed', {
	provider: ProviderName,
	threadId: ThreadId,
	message: Schema.String,
	retryability: Schema.optionalKey(Retryability),
	retryAfterMs: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
}) {}

export class DirectMessageOpenFailed extends Schema.TaggedError<DirectMessageOpenFailed>()('DirectMessageOpenFailed', {
	provider: ProviderName,
	tenant: TenantId,
	userId: UserId,
	message: Schema.String,
	retryability: Schema.optionalKey(Retryability),
}) {}

export class EditFailed extends Schema.TaggedError<EditFailed>()('EditFailed', {
	provider: ProviderName,
	threadId: ThreadId,
	message: Schema.String,
}) {}

export class DeleteFailed extends Schema.TaggedError<DeleteFailed>()('DeleteFailed', {
	provider: ProviderName,
	threadId: ThreadId,
	message: Schema.String,
}) {}

export class ReactionFailed extends Schema.TaggedError<ReactionFailed>()('ReactionFailed', {
	provider: ProviderName,
	threadId: ThreadId,
	message: Schema.String,
}) {}

export class StatusFailed extends Schema.TaggedError<StatusFailed>()('StatusFailed', {
	provider: ProviderName,
	threadId: ThreadId,
	message: Schema.String,
}) {}

export class HistoryFailed extends Schema.TaggedError<HistoryFailed>()('HistoryFailed', {
	provider: ProviderName,
	message: Schema.String,
	retryability: Schema.optionalKey(Retryability),
}) {}

export class ContextLoadFailed extends Schema.TaggedError<ContextLoadFailed>()('ContextLoadFailed', {
	provider: ProviderName,
	message: Schema.String,
	retryability: Schema.optionalKey(Retryability),
}) {}

export class UnsupportedContextScope extends Schema.TaggedError<UnsupportedContextScope>()('UnsupportedContextScope', {
	provider: ProviderName,
	scope: Schema.String,
	retryability: Schema.optionalKey(Retryability),
}) {}

export class ThreadGone extends Schema.TaggedError<ThreadGone>()('ThreadGone', {
	threadId: ThreadId,
	retryability: Schema.optionalKey(Retryability),
}) {}

export class ChannelGone extends Schema.TaggedError<ChannelGone>()('ChannelGone', {
	channelId: Schema.String,
	retryability: Schema.optionalKey(Retryability),
}) {}

export class MetadataFailed extends Schema.TaggedError<MetadataFailed>()('MetadataFailed', {
	provider: ProviderName,
	message: Schema.String,
	retryability: Schema.optionalKey(Retryability),
}) {}

export class SubjectFailed extends Schema.TaggedError<SubjectFailed>()('SubjectFailed', {
	provider: ProviderName,
	message: Schema.String,
}) {}

export class UserLookupFailed extends Schema.TaggedError<UserLookupFailed>()('UserLookupFailed', {
	provider: ProviderName,
	tenant: TenantId,
	userId: UserId,
	reason: Schema.Literals(['not_found', 'transport', 'api']),
	retryable: Schema.Boolean,
}) {}

export class FileReadFailed extends Schema.TaggedError<FileReadFailed>()('FileReadFailed', {
	provider: ProviderName,
	message: Schema.String,
}) {}

export class FileUploadFailed extends Schema.TaggedError<FileUploadFailed>()('FileUploadFailed', {
	provider: ProviderName,
	message: Schema.String,
}) {}

export class SubscriptionStoreError extends Schema.TaggedError<SubscriptionStoreError>()('SubscriptionStoreError', {
	operation: Schema.String,
	threadId: ThreadId,
}) {}

export class ChannelsRunError extends Schema.TaggedError<ChannelsRunError>()('ChannelsRunError', {
	operation: Schema.String,
	message: Schema.String,
	retryability: Schema.optionalKey(Retryability),
}) {}

export class ProviderAlreadyRegistered extends Schema.TaggedError<ProviderAlreadyRegistered>()(
	'ProviderAlreadyRegistered',
	{
		provider: ProviderName,
	},
) {}

export class IngressError extends Schema.TaggedError<IngressError>()('IngressError', {
	operation: Schema.String,
	provider: ProviderName,
	message: Schema.String,
}) {}

export class SlackIngressError extends Schema.TaggedError<SlackIngressError>()('SlackIngressError', {
	operation: Schema.NonEmptyString,
	reason: Schema.optionalKey(Schema.Literals(['unavailable', 'unexpected'])),
}) {}

export class OrganizationStoreError extends Schema.TaggedError<OrganizationStoreError>()('OrganizationStoreError', {
	source: Schema.String,
	tenant: TenantId,
	message: Schema.String,
}) {}

export class GateError extends Schema.TaggedError<GateError>()('GateError', {
	orgId: OrgId,
	source: Schema.String,
	message: Schema.String,
}) {}

export class ObserverError extends Schema.TaggedError<ObserverError>()('ObserverError', {
	operation: Schema.String,
	message: Schema.String,
}) {}

export class ConversationCoordinatorUnavailable extends Schema.TaggedError<ConversationCoordinatorUnavailable>()(
	'ConversationCoordinatorUnavailable',
	{
		operation: Schema.String,
		message: Schema.String,
	},
) {}

export class ConversationLeaseLost extends Schema.TaggedError<ConversationLeaseLost>()('ConversationLeaseLost', {
	threadId: ThreadId,
}) {}

export class ConversationSignalError extends Schema.TaggedError<ConversationSignalError>()('ConversationSignalError', {
	operation: Schema.String,
	message: Schema.String,
}) {}
