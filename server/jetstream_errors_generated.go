// Generated code, do not edit. See errors.json and run go generate to update

package server

import "strings"

const (
	// JSAccountResourcesExceededErr resource limits exceeded for account
	JSAccountResourcesExceededErr ErrorIdentifier = 10002

	// JSAtomicPublishDisabledErr atomic publish is disabled
	JSAtomicPublishDisabledErr ErrorIdentifier = 10174

	// JSAtomicPublishIncompleteBatchErr atomic publish batch is incomplete
	JSAtomicPublishIncompleteBatchErr ErrorIdentifier = 10176

	// JSAtomicPublishInvalidBatchIDErr atomic publish batch ID is invalid
	JSAtomicPublishInvalidBatchIDErr ErrorIdentifier = 10179

	// JSAtomicPublishMissingSeqErr atomic publish sequence is missing
	JSAtomicPublishMissingSeqErr ErrorIdentifier = 10175

	// JSAtomicPublishTooLargeBatchErr atomic publish batch is too large
	JSAtomicPublishTooLargeBatchErr ErrorIdentifier = 10199

	// JSAtomicPublishUnsupportedHeaderBatchErr atomic publish unsupported header used: {header}
	JSAtomicPublishUnsupportedHeaderBatchErr ErrorIdentifier = 10177

	// JSBadRequestErr bad request
	JSBadRequestErr ErrorIdentifier = 10003

	// JSClusterIncompleteErr incomplete results
	JSClusterIncompleteErr ErrorIdentifier = 10004

	// JSClusterNoPeersErrF Error causing no peers to be available ({err})
	JSClusterNoPeersErrF ErrorIdentifier = 10005

	// JSClusterNotActiveErr JetStream not in clustered mode
	JSClusterNotActiveErr ErrorIdentifier = 10006

	// JSClusterNotAssignedErr JetStream cluster not assigned to this server
	JSClusterNotAssignedErr ErrorIdentifier = 10007

	// JSClusterNotAvailErr JetStream system temporarily unavailable
	JSClusterNotAvailErr ErrorIdentifier = 10008

	// JSClusterNotLeaderErr JetStream cluster can not handle request
	JSClusterNotLeaderErr ErrorIdentifier = 10009

	// JSClusterPeerNotMemberErr peer not a member
	JSClusterPeerNotMemberErr ErrorIdentifier = 10040

	// JSClusterRequiredErr JetStream clustering support required
	JSClusterRequiredErr ErrorIdentifier = 10010

	// JSClusterServerNotMemberErr server is not a member of the cluster
	JSClusterServerNotMemberErr ErrorIdentifier = 10044

	// JSClusterTagsErr tags placement not supported for operation
	JSClusterTagsErr ErrorIdentifier = 10011

	// JSClusterUnSupportFeatureErr not currently supported in clustered mode
	JSClusterUnSupportFeatureErr ErrorIdentifier = 10036

	// JSConsumerAckPolicyInvalidErr consumer ack policy invalid
	JSConsumerAckPolicyInvalidErr ErrorIdentifier = 10181

	// JSConsumerAckWaitNegativeErr consumer ack wait needs to be positive
	JSConsumerAckWaitNegativeErr ErrorIdentifier = 10183

	// JSConsumerAlreadyExists action CREATE is used for a existing consumer with a different config (consumer already exists)
	JSConsumerAlreadyExists ErrorIdentifier = 10148

	// JSConsumerBackOffNegativeErr consumer backoff needs to be positive
	JSConsumerBackOffNegativeErr ErrorIdentifier = 10184

	// JSConsumerBadDurableNameErr durable name can not contain '.', '*', '>'
	JSConsumerBadDurableNameErr ErrorIdentifier = 10103

	// JSConsumerConfigRequiredErr consumer config required
	JSConsumerConfigRequiredErr ErrorIdentifier = 10078

	// JSConsumerCreateDurableAndNameMismatch Consumer Durable and Name have to be equal if both are provided
	JSConsumerCreateDurableAndNameMismatch ErrorIdentifier = 10132

	// JSConsumerCreateErrF General consumer creation failure string ({err})
	JSConsumerCreateErrF ErrorIdentifier = 10012

	// JSConsumerCreateFilterSubjectMismatchErr Consumer create request did not match filtered subject from create subject
	JSConsumerCreateFilterSubjectMismatchErr ErrorIdentifier = 10131

	// JSConsumerDeliverCycleErr consumer deliver subject forms a cycle
	JSConsumerDeliverCycleErr ErrorIdentifier = 10081

	// JSConsumerDeliverToWildcardsErr consumer deliver subject has wildcards
	JSConsumerDeliverToWildcardsErr ErrorIdentifier = 10079

	// JSConsumerDescriptionTooLongErrF consumer description is too long, maximum allowed is {max}
	JSConsumerDescriptionTooLongErrF ErrorIdentifier = 10107

	// JSConsumerDirectRequiresEphemeralErr consumer direct requires an ephemeral consumer
	JSConsumerDirectRequiresEphemeralErr ErrorIdentifier = 10091

	// JSConsumerDirectRequiresPushErr consumer direct requires a push based consumer
	JSConsumerDirectRequiresPushErr ErrorIdentifier = 10090

	// JSConsumerDoesNotExist action UPDATE is used for a nonexisting consumer (consumer does not exist)
	JSConsumerDoesNotExist ErrorIdentifier = 10149

	// JSConsumerDuplicateFilterSubjects consumer cannot have both FilterSubject and FilterSubjects specified
	JSConsumerDuplicateFilterSubjects ErrorIdentifier = 10136

	// JSConsumerDurableNameNotInSubjectErr consumer expected to be durable but no durable name set in subject
	JSConsumerDurableNameNotInSubjectErr ErrorIdentifier = 10016

	// JSConsumerDurableNameNotMatchSubjectErr consumer name in subject does not match durable name in request
	JSConsumerDurableNameNotMatchSubjectErr ErrorIdentifier = 10017

	// JSConsumerDurableNameNotSetErr consumer expected to be durable but a durable name was not set
	JSConsumerDurableNameNotSetErr ErrorIdentifier = 10018

	// JSConsumerEmptyFilter consumer filter in FilterSubjects cannot be empty
	JSConsumerEmptyFilter ErrorIdentifier = 10139

	// JSConsumerEmptyGroupName Group name cannot be an empty string
	JSConsumerEmptyGroupName ErrorIdentifier = 10161

	// JSConsumerEphemeralWithDurableInSubjectErr consumer expected to be ephemeral but detected a durable name set in subject
	JSConsumerEphemeralWithDurableInSubjectErr ErrorIdentifier = 10019

	// JSConsumerEphemeralWithDurableNameErr consumer expected to be ephemeral but a durable name was set in request
	JSConsumerEphemeralWithDurableNameErr ErrorIdentifier = 10020

	// JSConsumerExistingActiveErr consumer already exists and is still active
	JSConsumerExistingActiveErr ErrorIdentifier = 10105

	// JSConsumerFCRequiresPushErr consumer flow control requires a push based consumer
	JSConsumerFCRequiresPushErr ErrorIdentifier = 10089

	// JSConsumerFilterNotSubsetErr consumer filter subject is not a valid subset of the interest subjects
	JSConsumerFilterNotSubsetErr ErrorIdentifier = 10093

	// JSConsumerHBRequiresPushErr consumer idle heartbeat requires a push based consumer
	JSConsumerHBRequiresPushErr ErrorIdentifier = 10088

	// JSConsumerInactiveThresholdExcess consumer inactive threshold exceeds system limit of {limit}
	JSConsumerInactiveThresholdExcess ErrorIdentifier = 10153

	// JSConsumerInvalidDeliverSubject invalid push consumer deliver subject
	JSConsumerInvalidDeliverSubject ErrorIdentifier = 10112

	// JSConsumerInvalidGroupNameErr Valid priority group name must match A-Z, a-z, 0-9, -_/=)+ and may not exceed 16 characters
	JSConsumerInvalidGroupNameErr ErrorIdentifier = 10162

	// JSConsumerInvalidPolicyErrF Generic delivery policy error ({err})
	JSConsumerInvalidPolicyErrF ErrorIdentifier = 10094

	// JSConsumerInvalidPriorityGroupErr Provided priority group does not exist for this consumer
	JSConsumerInvalidPriorityGroupErr ErrorIdentifier = 10160

	// JSConsumerInvalidSamplingErrF failed to parse consumer sampling configuration: {err}
	JSConsumerInvalidSamplingErrF ErrorIdentifier = 10095

	// JSConsumerMaxDeliverBackoffErr max deliver is required to be > length of backoff values
	JSConsumerMaxDeliverBackoffErr ErrorIdentifier = 10116

	// JSConsumerMaxPendingAckExcessErrF consumer max ack pending exceeds system limit of {limit}
	JSConsumerMaxPendingAckExcessErrF ErrorIdentifier = 10121

	// JSConsumerMaxPendingAckPolicyRequiredErr consumer requires ack policy for max ack pending
	JSConsumerMaxPendingAckPolicyRequiredErr ErrorIdentifier = 10082

	// JSConsumerMaxRequestBatchExceededF consumer max request batch exceeds server limit of {limit}
	JSConsumerMaxRequestBatchExceededF ErrorIdentifier = 10125

	// JSConsumerMaxRequestBatchNegativeErr consumer max request batch needs to be > 0
	JSConsumerMaxRequestBatchNegativeErr ErrorIdentifier = 10114

	// JSConsumerMaxRequestExpiresTooSmall consumer max request expires needs to be >= 1ms
	JSConsumerMaxRequestExpiresTooSmall ErrorIdentifier = 10115

	// JSConsumerMaxWaitingNegativeErr consumer max waiting needs to be positive
	JSConsumerMaxWaitingNegativeErr ErrorIdentifier = 10087

	// JSConsumerMetadataLengthErrF consumer metadata exceeds maximum size of {limit}
	JSConsumerMetadataLengthErrF ErrorIdentifier = 10135

	// JSConsumerMultipleFiltersNotAllowed consumer with multiple subject filters cannot use subject based API
	JSConsumerMultipleFiltersNotAllowed ErrorIdentifier = 10137

	// JSConsumerNameContainsPathSeparatorsErr Consumer name can not contain path separators
	JSConsumerNameContainsPathSeparatorsErr ErrorIdentifier = 10127

	// JSConsumerNameExistErr consumer name already in use
	JSConsumerNameExistErr ErrorIdentifier = 10013

	// JSConsumerNameTooLongErrF consumer name is too long, maximum allowed is {max}
	JSConsumerNameTooLongErrF ErrorIdentifier = 10102

	// JSConsumerNotFoundErr consumer not found
	JSConsumerNotFoundErr ErrorIdentifier = 10014

	// JSConsumerOfflineErr consumer is offline
	JSConsumerOfflineErr ErrorIdentifier = 10119

	// JSConsumerOfflineReasonErrF consumer is offline: {err}
	JSConsumerOfflineReasonErrF ErrorIdentifier = 10195

	// JSConsumerOnMappedErr consumer direct on a mapped consumer
	JSConsumerOnMappedErr ErrorIdentifier = 10092

	// JSConsumerOverlappingSubjectFilters consumer subject filters cannot overlap
	JSConsumerOverlappingSubjectFilters ErrorIdentifier = 10138

	// JSConsumerPinnedTTLWithoutPriorityPolicyNone PinnedTTL cannot be set when PriorityPolicy is none
	JSConsumerPinnedTTLWithoutPriorityPolicyNone ErrorIdentifier = 10197

	// JSConsumerPriorityGroupWithPolicyNone consumer can not have priority groups when policy is none
	JSConsumerPriorityGroupWithPolicyNone ErrorIdentifier = 10196

	// JSConsumerPriorityPolicyWithoutGroup Setting PriorityPolicy requires at least one PriorityGroup to be set
	JSConsumerPriorityPolicyWithoutGroup ErrorIdentifier = 10159

	// JSConsumerPullNotDurableErr consumer in pull mode requires a durable name
	JSConsumerPullNotDurableErr ErrorIdentifier = 10085

	// JSConsumerPullRequiresAckErr consumer in pull mode requires explicit ack policy on workqueue stream
	JSConsumerPullRequiresAckErr ErrorIdentifier = 10084

	// JSConsumerPullWithRateLimitErr consumer in pull mode can not have rate limit set
	JSConsumerPullWithRateLimitErr ErrorIdentifier = 10086

	// JSConsumerPushMaxWaitingErr consumer in push mode can not set max waiting
	JSConsumerPushMaxWaitingErr ErrorIdentifier = 10080

	// JSConsumerPushWithPriorityGroupErr priority groups can not be used with push consumers
	JSConsumerPushWithPriorityGroupErr ErrorIdentifier = 10178

	// JSConsumerReplacementWithDifferentNameErr consumer replacement durable config not the same
	JSConsumerReplacementWithDifferentNameErr ErrorIdentifier = 10106

	// JSConsumerReplayPolicyInvalidErr consumer replay policy invalid
	JSConsumerReplayPolicyInvalidErr ErrorIdentifier = 10182

	// JSConsumerReplicasExceedsStream consumer config replica count exceeds parent stream
	JSConsumerReplicasExceedsStream ErrorIdentifier = 10126

	// JSConsumerReplicasShouldMatchStream consumer config replicas must match interest retention stream's replicas
	JSConsumerReplicasShouldMatchStream ErrorIdentifier = 10134

	// JSConsumerSmallHeartbeatErr consumer idle heartbeat needs to be >= 100ms
	JSConsumerSmallHeartbeatErr ErrorIdentifier = 10083

	// JSConsumerStoreFailedErrF error creating store for consumer: {err}
	JSConsumerStoreFailedErrF ErrorIdentifier = 10104

	// JSConsumerWQConsumerNotDeliverAllErr consumer must be deliver all on workqueue stream
	JSConsumerWQConsumerNotDeliverAllErr ErrorIdentifier = 10101

	// JSConsumerWQConsumerNotUniqueErr filtered consumer not unique on workqueue stream
	JSConsumerWQConsumerNotUniqueErr ErrorIdentifier = 10100

	// JSConsumerWQMultipleUnfilteredErr multiple non-filtered consumers not allowed on workqueue stream
	JSConsumerWQMultipleUnfilteredErr ErrorIdentifier = 10099

	// JSConsumerWQRequiresExplicitAckErr workqueue stream requires explicit ack
	JSConsumerWQRequiresExplicitAckErr ErrorIdentifier = 10098

	// JSConsumerWithFlowControlNeedsHeartbeats consumer with flow control also needs heartbeats
	JSConsumerWithFlowControlNeedsHeartbeats ErrorIdentifier = 10108

	// JSInsufficientResourcesErr insufficient resources
	JSInsufficientResourcesErr ErrorIdentifier = 10023

	// JSInvalidJSONErr invalid JSON: {err}
	JSInvalidJSONErr ErrorIdentifier = 10025

	// JSMaximumConsumersLimitErr maximum consumers limit reached
	JSMaximumConsumersLimitErr ErrorIdentifier = 10026

	// JSMaximumStreamsLimitErr maximum number of streams reached
	JSMaximumStreamsLimitErr ErrorIdentifier = 10027

	// JSMemoryResourcesExceededErr insufficient memory resources available
	JSMemoryResourcesExceededErr ErrorIdentifier = 10028

	// JSMessageCounterBrokenErr message counter is broken
	JSMessageCounterBrokenErr ErrorIdentifier = 10172

	// JSMessageIncrDisabledErr message counters is disabled
	JSMessageIncrDisabledErr ErrorIdentifier = 10168

	// JSMessageIncrInvalidErr message counter increment is invalid
	JSMessageIncrInvalidErr ErrorIdentifier = 10171

	// JSMessageIncrMissingErr message counter increment is missing
	JSMessageIncrMissingErr ErrorIdentifier = 10169

	// JSMessageIncrPayloadErr message counter has payload
	JSMessageIncrPayloadErr ErrorIdentifier = 10170

	// JSMessageSchedulesDisabledErr message schedules is disabled
	JSMessageSchedulesDisabledErr ErrorIdentifier = 10188

	// JSMessageSchedulesPatternInvalidErr message schedules pattern is invalid
	JSMessageSchedulesPatternInvalidErr ErrorIdentifier = 10189

	// JSMessageSchedulesRollupInvalidErr message schedules invalid rollup
	JSMessageSchedulesRollupInvalidErr ErrorIdentifier = 10192

	// JSMessageSchedulesTTLInvalidErr message schedules invalid per-message TTL
	JSMessageSchedulesTTLInvalidErr ErrorIdentifier = 10191

	// JSMessageSchedulesTargetInvalidErr message schedules target is invalid
	JSMessageSchedulesTargetInvalidErr ErrorIdentifier = 10190

	// JSMessageTTLDisabledErr per-message TTL is disabled
	JSMessageTTLDisabledErr ErrorIdentifier = 10166

	// JSMessageTTLInvalidErr invalid per-message TTL
	JSMessageTTLInvalidErr ErrorIdentifier = 10165

	// JSMirrorConsumerSetupFailedErrF generic mirror consumer setup failure string ({err})
	JSMirrorConsumerSetupFailedErrF ErrorIdentifier = 10029

	// JSMirrorInvalidStreamName mirrored stream name is invalid
	JSMirrorInvalidStreamName ErrorIdentifier = 10142

	// JSMirrorInvalidSubjectFilter mirror transform source: {err}
	JSMirrorInvalidSubjectFilter ErrorIdentifier = 10151

	// JSMirrorInvalidTransformDestination mirror transform: {err}
	JSMirrorInvalidTransformDestination ErrorIdentifier = 10154

	// JSMirrorMaxMessageSizeTooBigErr stream mirror must have max message size >= source
	JSMirrorMaxMessageSizeTooBigErr ErrorIdentifier = 10030

	// JSMirrorMultipleFiltersNotAllowed mirror with multiple subject transforms cannot also have a single subject filter
	JSMirrorMultipleFiltersNotAllowed ErrorIdentifier = 10150

	// JSMirrorOverlappingSubjectFilters mirror subject filters can not overlap
	JSMirrorOverlappingSubjectFilters ErrorIdentifier = 10152

	// JSMirrorWithAtomicPublishErr stream mirrors can not also use atomic publishing
	JSMirrorWithAtomicPublishErr ErrorIdentifier = 10198

	// JSMirrorWithCountersErr stream mirrors can not also calculate counters
	JSMirrorWithCountersErr ErrorIdentifier = 10173

	// JSMirrorWithFirstSeqErr stream mirrors can not have first sequence configured
	JSMirrorWithFirstSeqErr ErrorIdentifier = 10143

	// JSMirrorWithMsgSchedulesErr stream mirrors can not also schedule messages
	JSMirrorWithMsgSchedulesErr ErrorIdentifier = 10186

	// JSMirrorWithSourcesErr stream mirrors can not also contain other sources
	JSMirrorWithSourcesErr ErrorIdentifier = 10031

	// JSMirrorWithStartSeqAndTimeErr stream mirrors can not have both start seq and start time configured
	JSMirrorWithStartSeqAndTimeErr ErrorIdentifier = 10032

	// JSMirrorWithSubjectFiltersErr stream mirrors can not contain filtered subjects
	JSMirrorWithSubjectFiltersErr ErrorIdentifier = 10033

	// JSMirrorWithSubjectsErr stream mirrors can not contain subjects
	JSMirrorWithSubjectsErr ErrorIdentifier = 10034

	// JSNoAccountErr account not found
	JSNoAccountErr ErrorIdentifier = 10035

	// JSNoLimitsErr no JetStream default or applicable tiered limit present
	JSNoLimitsErr ErrorIdentifier = 10120

	// JSNoMessageFoundErr no message found
	JSNoMessageFoundErr ErrorIdentifier = 10037

	// JSNotEmptyRequestErr expected an empty request payload
	JSNotEmptyRequestErr ErrorIdentifier = 10038

	// JSNotEnabledErr JetStream not enabled
	JSNotEnabledErr ErrorIdentifier = 10076

	// JSNotEnabledForAccountErr JetStream not enabled for account
	JSNotEnabledForAccountErr ErrorIdentifier = 10039

	// JSPedanticErrF pedantic mode: {err}
	JSPedanticErrF ErrorIdentifier = 10157

	// JSPeerRemapErr peer remap failed
	JSPeerRemapErr ErrorIdentifier = 10075

	// JSRaftGeneralErrF General RAFT error string ({err})
	JSRaftGeneralErrF ErrorIdentifier = 10041

	// JSReplicasCountCannotBeNegative replicas count cannot be negative
	JSReplicasCountCannotBeNegative ErrorIdentifier = 10133

	// JSRequiredApiLevelErr JetStream minimum api level required
	JSRequiredApiLevelErr ErrorIdentifier = 10185

	// JSRestoreSubscribeFailedErrF JetStream unable to subscribe to restore snapshot {subject}: {err}
	JSRestoreSubscribeFailedErrF ErrorIdentifier = 10042

	// JSSequenceNotFoundErrF sequence {seq} not found
	JSSequenceNotFoundErrF ErrorIdentifier = 10043

	// JSSnapshotDeliverSubjectInvalidErr deliver subject not valid
	JSSnapshotDeliverSubjectInvalidErr ErrorIdentifier = 10015

	// JSSourceConsumerSetupFailedErrF General source consumer setup failure string ({err})
	JSSourceConsumerSetupFailedErrF ErrorIdentifier = 10045

	// JSSourceDuplicateDetected source stream, filter and transform (plus external if present) must form a unique combination (duplicate source configuration detected)
	JSSourceDuplicateDetected ErrorIdentifier = 10140

	// JSSourceInvalidStreamName sourced stream name is invalid
	JSSourceInvalidStreamName ErrorIdentifier = 10141

	// JSSourceInvalidSubjectFilter source transform source: {err}
	JSSourceInvalidSubjectFilter ErrorIdentifier = 10145

	// JSSourceInvalidTransformDestination source transform: {err}
	JSSourceInvalidTransformDestination ErrorIdentifier = 10146

	// JSSourceMaxMessageSizeTooBigErr stream source must have max message size >= target
	JSSourceMaxMessageSizeTooBigErr ErrorIdentifier = 10046

	// JSSourceMultipleFiltersNotAllowed source with multiple subject transforms cannot also have a single subject filter
	JSSourceMultipleFiltersNotAllowed ErrorIdentifier = 10144

	// JSSourceOverlappingSubjectFilters source filters can not overlap
	JSSourceOverlappingSubjectFilters ErrorIdentifier = 10147

	// JSSourceWithMsgSchedulesErr stream source can not also schedule messages
	JSSourceWithMsgSchedulesErr ErrorIdentifier = 10187

	// JSStorageResourcesExceededErr insufficient storage resources available
	JSStorageResourcesExceededErr ErrorIdentifier = 10047

	// JSStreamAssignmentErrF Generic stream assignment error string ({err})
	JSStreamAssignmentErrF ErrorIdentifier = 10048

	// JSStreamCreateErrF Generic stream creation error string ({err})
	JSStreamCreateErrF ErrorIdentifier = 10049

	// JSStreamDeleteErrF General stream deletion error string ({err})
	JSStreamDeleteErrF ErrorIdentifier = 10050

	// JSStreamDuplicateMessageConflict duplicate message id is in process
	JSStreamDuplicateMessageConflict ErrorIdentifier = 10158

	// JSStreamExpectedLastSeqPerSubjectInvalid missing sequence for expected last sequence per subject
	JSStreamExpectedLastSeqPerSubjectInvalid ErrorIdentifier = 10193

	// JSStreamExpectedLastSeqPerSubjectNotReady expected last sequence per subject temporarily unavailable
	JSStreamExpectedLastSeqPerSubjectNotReady ErrorIdentifier = 10163

	// JSStreamExternalApiOverlapErrF stream external api prefix {prefix} must not overlap with {subject}
	JSStreamExternalApiOverlapErrF ErrorIdentifier = 10021

	// JSStreamExternalDelPrefixOverlapsErrF stream external delivery prefix {prefix} overlaps with stream subject {subject}
	JSStreamExternalDelPrefixOverlapsErrF ErrorIdentifier = 10022

	// JSStreamGeneralErrorF General stream failure string ({err})
	JSStreamGeneralErrorF ErrorIdentifier = 10051

	// JSStreamHeaderExceedsMaximumErr header size exceeds maximum allowed of 64k
	JSStreamHeaderExceedsMaximumErr ErrorIdentifier = 10097

	// JSStreamInfoMaxSubjectsErr subject details would exceed maximum allowed
	JSStreamInfoMaxSubjectsErr ErrorIdentifier = 10117

	// JSStreamInvalidConfigF Stream configuration validation error string ({err})
	JSStreamInvalidConfigF ErrorIdentifier = 10052

	// JSStreamInvalidErr stream not valid
	JSStreamInvalidErr ErrorIdentifier = 10096

	// JSStreamInvalidExternalDeliverySubjErrF stream external delivery prefix {prefix} must not contain wildcards
	JSStreamInvalidExternalDeliverySubjErrF ErrorIdentifier = 10024

	// JSStreamLimitsErrF General stream limits exceeded error string ({err})
	JSStreamLimitsErrF ErrorIdentifier = 10053

	// JSStreamMaxBytesRequired account requires a stream config to have max bytes set
	JSStreamMaxBytesRequired ErrorIdentifier = 10113

	// JSStreamMaxStreamBytesExceeded stream max bytes exceeds account limit max stream bytes
	JSStreamMaxStreamBytesExceeded ErrorIdentifier = 10122

	// JSStreamMessageExceedsMaximumErr message size exceeds maximum allowed
	JSStreamMessageExceedsMaximumErr ErrorIdentifier = 10054

	// JSStreamMinLastSeqErr min last sequence
	JSStreamMinLastSeqErr ErrorIdentifier = 10180

	// JSStreamMirrorNotUpdatableErr stream mirror configuration can not be updated
	JSStreamMirrorNotUpdatableErr ErrorIdentifier = 10055

	// JSStreamMismatchErr stream name in subject does not match request
	JSStreamMismatchErr ErrorIdentifier = 10056

	// JSStreamMoveAndScaleErr can not move and scale a stream in a single update
	JSStreamMoveAndScaleErr ErrorIdentifier = 10123

	// JSStreamMoveInProgressF stream move already in progress: {msg}
	JSStreamMoveInProgressF ErrorIdentifier = 10124

	// JSStreamMoveNotInProgress stream move not in progress
	JSStreamMoveNotInProgress ErrorIdentifier = 10129

	// JSStreamMsgDeleteFailedF Generic message deletion failure error string ({err})
	JSStreamMsgDeleteFailedF ErrorIdentifier = 10057

	// JSStreamNameContainsPathSeparatorsErr Stream name can not contain path separators
	JSStreamNameContainsPathSeparatorsErr ErrorIdentifier = 10128

	// JSStreamNameExistErr stream name already in use with a different configuration
	JSStreamNameExistErr ErrorIdentifier = 10058

	// JSStreamNameExistRestoreFailedErr stream name already in use, cannot restore
	JSStreamNameExistRestoreFailedErr ErrorIdentifier = 10130

	// JSStreamNotFoundErr stream not found
	JSStreamNotFoundErr ErrorIdentifier = 10059

	// JSStreamNotMatchErr expected stream does not match
	JSStreamNotMatchErr ErrorIdentifier = 10060

	// JSStreamOfflineErr stream is offline
	JSStreamOfflineErr ErrorIdentifier = 10118

	// JSStreamOfflineReasonErrF stream is offline: {err}
	JSStreamOfflineReasonErrF ErrorIdentifier = 10194

	// JSStreamPurgeFailedF Generic stream purge failure error string ({err})
	JSStreamPurgeFailedF ErrorIdentifier = 10110

	// JSStreamReplicasNotSupportedErr replicas > 1 not supported in non-clustered mode
	JSStreamReplicasNotSupportedErr ErrorIdentifier = 10074

	// JSStreamReplicasNotUpdatableErr Replicas configuration can not be updated
	JSStreamReplicasNotUpdatableErr ErrorIdentifier = 10061

	// JSStreamRestoreErrF restore failed: {err}
	JSStreamRestoreErrF ErrorIdentifier = 10062

	// JSStreamRollupFailedF Generic stream rollup failure error string ({err})
	JSStreamRollupFailedF ErrorIdentifier = 10111

	// JSStreamSealedErr invalid operation on sealed stream
	JSStreamSealedErr ErrorIdentifier = 10109

	// JSStreamSequenceNotMatchErr expected stream sequence does not match
	JSStreamSequenceNotMatchErr ErrorIdentifier = 10063

	// JSStreamSnapshotErrF snapshot failed: {err}
	JSStreamSnapshotErrF ErrorIdentifier = 10064

	// JSStreamStoreFailedF Generic error when storing a message failed ({err})
	JSStreamStoreFailedF ErrorIdentifier = 10077

	// JSStreamSubjectOverlapErr subjects overlap with an existing stream
	JSStreamSubjectOverlapErr ErrorIdentifier = 10065

	// JSStreamTemplateCreateErrF Generic template creation failed string ({err})
	JSStreamTemplateCreateErrF ErrorIdentifier = 10066

	// JSStreamTemplateDeleteErrF Generic stream template deletion failed error string ({err})
	JSStreamTemplateDeleteErrF ErrorIdentifier = 10067

	// JSStreamTemplateNotFoundErr template not found
	JSStreamTemplateNotFoundErr ErrorIdentifier = 10068

	// JSStreamTooManyRequests too many requests
	JSStreamTooManyRequests ErrorIdentifier = 10167

	// JSStreamTransformInvalidDestination stream transform: {err}
	JSStreamTransformInvalidDestination ErrorIdentifier = 10156

	// JSStreamTransformInvalidSource stream transform source: {err}
	JSStreamTransformInvalidSource ErrorIdentifier = 10155

	// JSStreamUpdateErrF Generic stream update error string ({err})
	JSStreamUpdateErrF ErrorIdentifier = 10069

	// JSStreamWrongLastMsgIDErrF wrong last msg ID: {id}
	JSStreamWrongLastMsgIDErrF ErrorIdentifier = 10070

	// JSStreamWrongLastSequenceConstantErr wrong last sequence
	JSStreamWrongLastSequenceConstantErr ErrorIdentifier = 10164

	// JSStreamWrongLastSequenceErrF wrong last sequence: {seq}
	JSStreamWrongLastSequenceErrF ErrorIdentifier = 10071

	// JSTempStorageFailedErr JetStream unable to open temp storage for restore
	JSTempStorageFailedErr ErrorIdentifier = 10072

	// JSTemplateNameNotMatchSubjectErr template name in subject does not match request
	JSTemplateNameNotMatchSubjectErr ErrorIdentifier = 10073
)

var (
	ApiErrors = map[ErrorIdentifier]*ApiError{
		JSAccountResourcesExceededErr:                {Code: 400, ErrCode: 10002, Description: "resource limits exceeded for account"},
		JSAtomicPublishDisabledErr:                   {Code: 400, ErrCode: 10174, Description: "atomic publish is disabled"},
		JSAtomicPublishIncompleteBatchErr:            {Code: 400, ErrCode: 10176, Description: "atomic publish batch is incomplete"},
		JSAtomicPublishInvalidBatchIDErr:             {Code: 400, ErrCode: 10179, Description: "atomic publish batch ID is invalid"},
		JSAtomicPublishMissingSeqErr:                 {Code: 400, ErrCode: 10175, Description: "atomic publish sequence is missing"},
		JSAtomicPublishTooLargeBatchErr:              {Code: 400, ErrCode: 10199, Description: "atomic publish batch is too large"},
		JSAtomicPublishUnsupportedHeaderBatchErr:     {Code: 400, ErrCode: 10177, Description: "atomic publish unsupported header used: {header}"},
		JSBadRequestErr:                              {Code: 400, ErrCode: 10003, Description: "bad request"},
		JSClusterIncompleteErr:                       {Code: 503, ErrCode: 10004, Description: "incomplete results"},
		JSClusterNoPeersErrF:                         {Code: 400, ErrCode: 10005, Description: "{err}"},
		JSClusterNotActiveErr:                        {Code: 500, ErrCode: 10006, Description: "JetStream not in clustered mode"},
		JSClusterNotAssignedErr:                      {Code: 500, ErrCode: 10007, Description: "JetStream cluster not assigned to this server"},
		JSClusterNotAvailErr:                         {Code: 503, ErrCode: 10008, Description: "JetStream system temporarily unavailable"},
		JSClusterNotLeaderErr:                        {Code: 500, ErrCode: 10009, Description: "JetStream cluster can not handle request"},
		JSClusterPeerNotMemberErr:                    {Code: 400, ErrCode: 10040, Description: "peer not a member"},
		JSClusterRequiredErr:                         {Code: 503, ErrCode: 10010, Description: "JetStream clustering support required"},
		JSClusterServerNotMemberErr:                  {Code: 400, ErrCode: 10044, Description: "server is not a member of the cluster"},
		JSClusterTagsErr:                             {Code: 400, ErrCode: 10011, Description: "tags placement not supported for operation"},
		JSClusterUnSupportFeatureErr:                 {Code: 503, ErrCode: 10036, Description: "not currently supported in clustered mode"},
		JSConsumerAckPolicyInvalidErr:                {Code: 400, ErrCode: 10181, Description: "consumer ack policy invalid"},
		JSConsumerAckWaitNegativeErr:                 {Code: 400, ErrCode: 10183, Description: "consumer ack wait needs to be positive"},
		JSConsumerAlreadyExists:                      {Code: 400, ErrCode: 10148, Description: "consumer already exists"},
		JSConsumerBackOffNegativeErr:                 {Code: 400, ErrCode: 10184, Description: "consumer backoff needs to be positive"},
		JSConsumerBadDurableNameErr:                  {Code: 400, ErrCode: 10103, Description: "durable name can not contain '.', '*', '>'"},
		JSConsumerConfigRequiredErr:                  {Code: 400, ErrCode: 10078, Description: "consumer config required"},
		JSConsumerCreateDurableAndNameMismatch:       {Code: 400, ErrCode: 10132, Description: "Consumer Durable and Name have to be equal if both are provided"},
		JSConsumerCreateErrF:                         {Code: 500, ErrCode: 10012, Description: "{err}"},
		JSConsumerCreateFilterSubjectMismatchErr:     {Code: 400, ErrCode: 10131, Description: "Consumer create request did not match filtered subject from create subject"},
		JSConsumerDeliverCycleErr:                    {Code: 400, ErrCode: 10081, Description: "consumer deliver subject forms a cycle"},
		JSConsumerDeliverToWildcardsErr:              {Code: 400, ErrCode: 10079, Description: "consumer deliver subject has wildcards"},
		JSConsumerDescriptionTooLongErrF:             {Code: 400, ErrCode: 10107, Description: "consumer description is too long, maximum allowed is {max}"},
		JSConsumerDirectRequiresEphemeralErr:         {Code: 400, ErrCode: 10091, Description: "consumer direct requires an ephemeral consumer"},
		JSConsumerDirectRequiresPushErr:              {Code: 400, ErrCode: 10090, Description: "consumer direct requires a push based consumer"},
		JSConsumerDoesNotExist:                       {Code: 400, ErrCode: 10149, Description: "consumer does not exist"},
		JSConsumerDuplicateFilterSubjects:            {Code: 400, ErrCode: 10136, Description: "consumer cannot have both FilterSubject and FilterSubjects specified"},
		JSConsumerDurableNameNotInSubjectErr:         {Code: 400, ErrCode: 10016, Description: "consumer expected to be durable but no durable name set in subject"},
		JSConsumerDurableNameNotMatchSubjectErr:      {Code: 400, ErrCode: 10017, Description: "consumer name in subject does not match durable name in request"},
		JSConsumerDurableNameNotSetErr:               {Code: 400, ErrCode: 10018, Description: "consumer expected to be durable but a durable name was not set"},
		JSConsumerEmptyFilter:                        {Code: 400, ErrCode: 10139, Description: "consumer filter in FilterSubjects cannot be empty"},
		JSConsumerEmptyGroupName:                     {Code: 400, ErrCode: 10161, Description: "Group name cannot be an empty string"},
		JSConsumerEphemeralWithDurableInSubjectErr:   {Code: 400, ErrCode: 10019, Description: "consumer expected to be ephemeral but detected a durable name set in subject"},
		JSConsumerEphemeralWithDurableNameErr:        {Code: 400, ErrCode: 10020, Description: "consumer expected to be ephemeral but a durable name was set in request"},
		JSConsumerExistingActiveErr:                  {Code: 400, ErrCode: 10105, Description: "consumer already exists and is still active"},
		JSConsumerFCRequiresPushErr:                  {Code: 400, ErrCode: 10089, Description: "consumer flow control requires a push based consumer"},
		JSConsumerFilterNotSubsetErr:                 {Code: 400, ErrCode: 10093, Description: "consumer filter subject is not a valid subset of the interest subjects"},
		JSConsumerHBRequiresPushErr:                  {Code: 400, ErrCode: 10088, Description: "consumer idle heartbeat requires a push based consumer"},
		JSConsumerInactiveThresholdExcess:            {Code: 400, ErrCode: 10153, Description: "consumer inactive threshold exceeds system limit of {limit}"},
		JSConsumerInvalidDeliverSubject:              {Code: 400, ErrCode: 10112, Description: "invalid push consumer deliver subject"},
		JSConsumerInvalidGroupNameErr:                {Code: 400, ErrCode: 10162, Description: "Valid priority group name must match A-Z, a-z, 0-9, -_/=)+ and may not exceed 16 characters"},
		JSConsumerInvalidPolicyErrF:                  {Code: 400, ErrCode: 10094, Description: "{err}"},
		JSConsumerInvalidPriorityGroupErr:            {Code: 400, ErrCode: 10160, Description: "Provided priority group does not exist for this consumer"},
		JSConsumerInvalidSamplingErrF:                {Code: 400, ErrCode: 10095, Description: "failed to parse consumer sampling configuration: {err}"},
		JSConsumerMaxDeliverBackoffErr:               {Code: 400, ErrCode: 10116, Description: "max deliver is required to be > length of backoff values"},
		JSConsumerMaxPendingAckExcessErrF:            {Code: 400, ErrCode: 10121, Description: "consumer max ack pending exceeds system limit of {limit}"},
		JSConsumerMaxPendingAckPolicyRequiredErr:     {Code: 400, ErrCode: 10082, Description: "consumer requires ack policy for max ack pending"},
		JSConsumerMaxRequestBatchExceededF:           {Code: 400, ErrCode: 10125, Description: "consumer max request batch exceeds server limit of {limit}"},
		JSConsumerMaxRequestBatchNegativeErr:         {Code: 400, ErrCode: 10114, Description: "consumer max request batch needs to be > 0"},
		JSConsumerMaxRequestExpiresTooSmall:          {Code: 400, ErrCode: 10115, Description: "consumer max request expires needs to be >= 1ms"},
		JSConsumerMaxWaitingNegativeErr:              {Code: 400, ErrCode: 10087, Description: "consumer max waiting needs to be positive"},
		JSConsumerMetadataLengthErrF:                 {Code: 400, ErrCode: 10135, Description: "consumer metadata exceeds maximum size of {limit}"},
		JSConsumerMultipleFiltersNotAllowed:          {Code: 400, ErrCode: 10137, Description: "consumer with multiple subject filters cannot use subject based API"},
		JSConsumerNameContainsPathSeparatorsErr:      {Code: 400, ErrCode: 10127, Description: "Consumer name can not contain path separators"},
		JSConsumerNameExistErr:                       {Code: 400, ErrCode: 10013, Description: "consumer name already in use"},
		JSConsumerNameTooLongErrF:                    {Code: 400, ErrCode: 10102, Description: "consumer name is too long, maximum allowed is {max}"},
		JSConsumerNotFoundErr:                        {Code: 404, ErrCode: 10014, Description: "consumer not found"},
		JSConsumerOfflineErr:                         {Code: 500, ErrCode: 10119, Description: "consumer is offline"},
		JSConsumerOfflineReasonErrF:                  {Code: 500, ErrCode: 10195, Description: "consumer is offline: {err}"},
		JSConsumerOnMappedErr:                        {Code: 400, ErrCode: 10092, Description: "consumer direct on a mapped consumer"},
		JSConsumerOverlappingSubjectFilters:          {Code: 400, ErrCode: 10138, Description: "consumer subject filters cannot overlap"},
		JSConsumerPinnedTTLWithoutPriorityPolicyNone: {Code: 400, ErrCode: 10197, Description: "PinnedTTL cannot be set when PriorityPolicy is none"},
		JSConsumerPriorityGroupWithPolicyNone:        {Code: 400, ErrCode: 10196, Description: "consumer can not have priority groups when policy is none"},
		JSConsumerPriorityPolicyWithoutGroup:         {Code: 400, ErrCode: 10159, Description: "Setting PriorityPolicy requires at least one PriorityGroup to be set"},
		JSConsumerPullNotDurableErr:                  {Code: 400, ErrCode: 10085, Description: "consumer in pull mode requires a durable name"},
		JSConsumerPullRequiresAckErr:                 {Code: 400, ErrCode: 10084, Description: "consumer in pull mode requires explicit ack policy on workqueue stream"},
		JSConsumerPullWithRateLimitErr:               {Code: 400, ErrCode: 10086, Description: "consumer in pull mode can not have rate limit set"},
		JSConsumerPushMaxWaitingErr:                  {Code: 400, ErrCode: 10080, Description: "consumer in push mode can not set max waiting"},
		JSConsumerPushWithPriorityGroupErr:           {Code: 400, ErrCode: 10178, Description: "priority groups can not be used with push consumers"},
		JSConsumerReplacementWithDifferentNameErr:    {Code: 400, ErrCode: 10106, Description: "consumer replacement durable config not the same"},
		JSConsumerReplayPolicyInvalidErr:             {Code: 400, ErrCode: 10182, Description: "consumer replay policy invalid"},
		JSConsumerReplicasExceedsStream:              {Code: 400, ErrCode: 10126, Description: "consumer config replica count exceeds parent stream"},
		JSConsumerReplicasShouldMatchStream:          {Code: 400, ErrCode: 10134, Description: "consumer config replicas must match interest retention stream's replicas"},
		JSConsumerSmallHeartbeatErr:                  {Code: 400, ErrCode: 10083, Description: "consumer idle heartbeat needs to be >= 100ms"},
		JSConsumerStoreFailedErrF:                    {Code: 500, ErrCode: 10104, Description: "error creating store for consumer: {err}"},
		JSConsumerWQConsumerNotDeliverAllErr:         {Code: 400, ErrCode: 10101, Description: "consumer must be deliver all on workqueue stream"},
		JSConsumerWQConsumerNotUniqueErr:             {Code: 400, ErrCode: 10100, Description: "filtered consumer not unique on workqueue stream"},
		JSConsumerWQMultipleUnfilteredErr:            {Code: 400, ErrCode: 10099, Description: "multiple non-filtered consumers not allowed on workqueue stream"},
		JSConsumerWQRequiresExplicitAckErr:           {Code: 400, ErrCode: 10098, Description: "workqueue stream requires explicit ack"},
		JSConsumerWithFlowControlNeedsHeartbeats:     {Code: 400, ErrCode: 10108, Description: "consumer with flow control also needs heartbeats"},
		JSInsufficientResourcesErr:                   {Code: 503, ErrCode: 10023, Description: "insufficient resources"},
		JSInvalidJSONErr:                             {Code: 400, ErrCode: 10025, Description: "invalid JSON: {err}"},
		JSMaximumConsumersLimitErr:                   {Code: 400, ErrCode: 10026, Description: "maximum consumers limit reached"},
		JSMaximumStreamsLimitErr:                     {Code: 400, ErrCode: 10027, Description: "maximum number of streams reached"},
		JSMemoryResourcesExceededErr:                 {Code: 500, ErrCode: 10028, Description: "insufficient memory resources available"},
		JSMessageCounterBrokenErr:                    {Code: 400, ErrCode: 10172, Description: "message counter is broken"},
		JSMessageIncrDisabledErr:                     {Code: 400, ErrCode: 10168, Description: "message counters is disabled"},
		JSMessageIncrInvalidErr:                      {Code: 400, ErrCode: 10171, Description: "message counter increment is invalid"},
		JSMessageIncrMissingErr:                      {Code: 400, ErrCode: 10169, Description: "message counter increment is missing"},
		JSMessageIncrPayloadErr:                      {Code: 400, ErrCode: 10170, Description: "message counter has payload"},
		JSMessageSchedulesDisabledErr:                {Code: 400, ErrCode: 10188, Description: "message schedules is disabled"},
		JSMessageSchedulesPatternInvalidErr:          {Code: 400, ErrCode: 10189, Description: "message schedules pattern is invalid"},
		JSMessageSchedulesRollupInvalidErr:           {Code: 400, ErrCode: 10192, Description: "message schedules invalid rollup"},
		JSMessageSchedulesTTLInvalidErr:              {Code: 400, ErrCode: 10191, Description: "message schedules invalid per-message TTL"},
		JSMessageSchedulesTargetInvalidErr:           {Code: 400, ErrCode: 10190, Description: "message schedules target is invalid"},
		JSMessageTTLDisabledErr:                      {Code: 400, ErrCode: 10166, Description: "per-message TTL is disabled"},
		JSMessageTTLInvalidErr:                       {Code: 400, ErrCode: 10165, Description: "invalid per-message TTL"},
		JSMirrorConsumerSetupFailedErrF:              {Code: 500, ErrCode: 10029, Description: "{err}"},
		JSMirrorInvalidStreamName:                    {Code: 400, ErrCode: 10142, Description: "mirrored stream name is invalid"},
		JSMirrorInvalidSubjectFilter:                 {Code: 400, ErrCode: 10151, Description: "mirror transform source: {err}"},
		JSMirrorInvalidTransformDestination:          {Code: 400, ErrCode: 10154, Description: "mirror transform: {err}"},
		JSMirrorMaxMessageSizeTooBigErr:              {Code: 400, ErrCode: 10030, Description: "stream mirror must have max message size >= source"},
		JSMirrorMultipleFiltersNotAllowed:            {Code: 400, ErrCode: 10150, Description: "mirror with multiple subject transforms cannot also have a single subject filter"},
		JSMirrorOverlappingSubjectFilters:            {Code: 400, ErrCode: 10152, Description: "mirror subject filters can not overlap"},
		JSMirrorWithAtomicPublishErr:                 {Code: 400, ErrCode: 10198, Description: "stream mirrors can not also use atomic publishing"},
		JSMirrorWithCountersErr:                      {Code: 400, ErrCode: 10173, Description: "stream mirrors can not also calculate counters"},
		JSMirrorWithFirstSeqErr:                      {Code: 400, ErrCode: 10143, Description: "stream mirrors can not have first sequence configured"},
		JSMirrorWithMsgSchedulesErr:                  {Code: 400, ErrCode: 10186, Description: "stream mirrors can not also schedule messages"},
		JSMirrorWithSourcesErr:                       {Code: 400, ErrCode: 10031, Description: "stream mirrors can not also contain other sources"},
		JSMirrorWithStartSeqAndTimeErr:               {Code: 400, ErrCode: 10032, Description: "stream mirrors can not have both start seq and start time configured"},
		JSMirrorWithSubjectFiltersErr:                {Code: 400, ErrCode: 10033, Description: "stream mirrors can not contain filtered subjects"},
		JSMirrorWithSubjectsErr:                      {Code: 400, ErrCode: 10034, Description: "stream mirrors can not contain subjects"},
		JSNoAccountErr:                               {Code: 503, ErrCode: 10035, Description: "account not found"},
		JSNoLimitsErr:                                {Code: 400, ErrCode: 10120, Description: "no JetStream default or applicable tiered limit present"},
		JSNoMessageFoundErr:                          {Code: 404, ErrCode: 10037, Description: "no message found"},
		JSNotEmptyRequestErr:                         {Code: 400, ErrCode: 10038, Description: "expected an empty request payload"},
		JSNotEnabledErr:                              {Code: 503, ErrCode: 10076, Description: "JetStream not enabled"},
		JSNotEnabledForAccountErr:                    {Code: 503, ErrCode: 10039, Description: "JetStream not enabled for account"},
		JSPedanticErrF:                               {Code: 400, ErrCode: 10157, Description: "pedantic mode: {err}"},
		JSPeerRemapErr:                               {Code: 503, ErrCode: 10075, Description: "peer remap failed"},
		JSRaftGeneralErrF:                            {Code: 500, ErrCode: 10041, Description: "{err}"},
		JSReplicasCountCannotBeNegative:              {Code: 400, ErrCode: 10133, Description: "replicas count cannot be negative"},
		JSRequiredApiLevelErr:                        {Code: 412, ErrCode: 10185, Description: "JetStream minimum api level required"},
		JSRestoreSubscribeFailedErrF:                 {Code: 500, ErrCode: 10042, Description: "JetStream unable to subscribe to restore snapshot {subject}: {err}"},
		JSSequenceNotFoundErrF:                       {Code: 400, ErrCode: 10043, Description: "sequence {seq} not found"},
		JSSnapshotDeliverSubjectInvalidErr:           {Code: 400, ErrCode: 10015, Description: "deliver subject not valid"},
		JSSourceConsumerSetupFailedErrF:              {Code: 500, ErrCode: 10045, Description: "{err}"},
		JSSourceDuplicateDetected:                    {Code: 400, ErrCode: 10140, Description: "duplicate source configuration detected"},
		JSSourceInvalidStreamName:                    {Code: 400, ErrCode: 10141, Description: "sourced stream name is invalid"},
		JSSourceInvalidSubjectFilter:                 {Code: 400, ErrCode: 10145, Description: "source transform source: {err}"},
		JSSourceInvalidTransformDestination:          {Code: 400, ErrCode: 10146, Description: "source transform: {err}"},
		JSSourceMaxMessageSizeTooBigErr:              {Code: 400, ErrCode: 10046, Description: "stream source must have max message size >= target"},
		JSSourceMultipleFiltersNotAllowed:            {Code: 400, ErrCode: 10144, Description: "source with multiple subject transforms cannot also have a single subject filter"},
		JSSourceOverlappingSubjectFilters:            {Code: 400, ErrCode: 10147, Description: "source filters can not overlap"},
		JSSourceWithMsgSchedulesErr:                  {Code: 400, ErrCode: 10187, Description: "stream source can not also schedule messages"},
		JSStorageResourcesExceededErr:                {Code: 500, ErrCode: 10047, Description: "insufficient storage resources available"},
		JSStreamAssignmentErrF:                       {Code: 500, ErrCode: 10048, Description: "{err}"},
		JSStreamCreateErrF:                           {Code: 500, ErrCode: 10049, Description: "{err}"},
		JSStreamDeleteErrF:                           {Code: 500, ErrCode: 10050, Description: "{err}"},
		JSStreamDuplicateMessageConflict:             {Code: 409, ErrCode: 10158, Description: "duplicate message id is in process"},
		JSStreamExpectedLastSeqPerSubjectInvalid:     {Code: 400, ErrCode: 10193, Description: "missing sequence for expected last sequence per subject"},
		JSStreamExpectedLastSeqPerSubjectNotReady:    {Code: 503, ErrCode: 10163, Description: "expected last sequence per subject temporarily unavailable"},
		JSStreamExternalApiOverlapErrF:               {Code: 400, ErrCode: 10021, Description: "stream external api prefix {prefix} must not overlap with {subject}"},
		JSStreamExternalDelPrefixOverlapsErrF:        {Code: 400, ErrCode: 10022, Description: "stream external delivery prefix {prefix} overlaps with stream subject {subject}"},
		JSStreamGeneralErrorF:                        {Code: 500, ErrCode: 10051, Description: "{err}"},
		JSStreamHeaderExceedsMaximumErr:              {Code: 400, ErrCode: 10097, Description: "header size exceeds maximum allowed of 64k"},
		JSStreamInfoMaxSubjectsErr:                   {Code: 500, ErrCode: 10117, Description: "subject details would exceed maximum allowed"},
		JSStreamInvalidConfigF:                       {Code: 500, ErrCode: 10052, Description: "{err}"},
		JSStreamInvalidErr:                           {Code: 500, ErrCode: 10096, Description: "stream not valid"},
		JSStreamInvalidExternalDeliverySubjErrF:      {Code: 400, ErrCode: 10024, Description: "stream external delivery prefix {prefix} must not contain wildcards"},
		JSStreamLimitsErrF:                           {Code: 500, ErrCode: 10053, Description: "{err}"},
		JSStreamMaxBytesRequired:                     {Code: 400, ErrCode: 10113, Description: "account requires a stream config to have max bytes set"},
		JSStreamMaxStreamBytesExceeded:               {Code: 400, ErrCode: 10122, Description: "stream max bytes exceeds account limit max stream bytes"},
		JSStreamMessageExceedsMaximumErr:             {Code: 400, ErrCode: 10054, Description: "message size exceeds maximum allowed"},
		JSStreamMinLastSeqErr:                        {Code: 412, ErrCode: 10180, Description: "min last sequence"},
		JSStreamMirrorNotUpdatableErr:                {Code: 400, ErrCode: 10055, Description: "stream mirror configuration can not be updated"},
		JSStreamMismatchErr:                          {Code: 400, ErrCode: 10056, Description: "stream name in subject does not match request"},
		JSStreamMoveAndScaleErr:                      {Code: 400, ErrCode: 10123, Description: "can not move and scale a stream in a single update"},
		JSStreamMoveInProgressF:                      {Code: 400, ErrCode: 10124, Description: "stream move already in progress: {msg}"},
		JSStreamMoveNotInProgress:                    {Code: 400, ErrCode: 10129, Description: "stream move not in progress"},
		JSStreamMsgDeleteFailedF:                     {Code: 500, ErrCode: 10057, Description: "{err}"},
		JSStreamNameContainsPathSeparatorsErr:        {Code: 400, ErrCode: 10128, Description: "Stream name can not contain path separators"},
		JSStreamNameExistErr:                         {Code: 400, ErrCode: 10058, Description: "stream name already in use with a different configuration"},
		JSStreamNameExistRestoreFailedErr:            {Code: 400, ErrCode: 10130, Description: "stream name already in use, cannot restore"},
		JSStreamNotFoundErr:                          {Code: 404, ErrCode: 10059, Description: "stream not found"},
		JSStreamNotMatchErr:                          {Code: 400, ErrCode: 10060, Description: "expected stream does not match"},
		JSStreamOfflineErr:                           {Code: 500, ErrCode: 10118, Description: "stream is offline"},
		JSStreamOfflineReasonErrF:                    {Code: 500, ErrCode: 10194, Description: "stream is offline: {err}"},
		JSStreamPurgeFailedF:                         {Code: 500, ErrCode: 10110, Description: "{err}"},
		JSStreamReplicasNotSupportedErr:              {Code: 500, ErrCode: 10074, Description: "replicas > 1 not supported in non-clustered mode"},
		JSStreamReplicasNotUpdatableErr:              {Code: 400, ErrCode: 10061, Description: "Replicas configuration can not be updated"},
		JSStreamRestoreErrF:                          {Code: 500, ErrCode: 10062, Description: "restore failed: {err}"},
		JSStreamRollupFailedF:                        {Code: 500, ErrCode: 10111, Description: "{err}"},
		JSStreamSealedErr:                            {Code: 400, ErrCode: 10109, Description: "invalid operation on sealed stream"},
		JSStreamSequenceNotMatchErr:                  {Code: 503, ErrCode: 10063, Description: "expected stream sequence does not match"},
		JSStreamSnapshotErrF:                         {Code: 500, ErrCode: 10064, Description: "snapshot failed: {err}"},
		JSStreamStoreFailedF:                         {Code: 503, ErrCode: 10077, Description: "{err}"},
		JSStreamSubjectOverlapErr:                    {Code: 400, ErrCode: 10065, Description: "subjects overlap with an existing stream"},
		JSStreamTemplateCreateErrF:                   {Code: 500, ErrCode: 10066, Description: "{err}"},
		JSStreamTemplateDeleteErrF:                   {Code: 500, ErrCode: 10067, Description: "{err}"},
		JSStreamTemplateNotFoundErr:                  {Code: 404, ErrCode: 10068, Description: "template not found"},
		JSStreamTooManyRequests:                      {Code: 429, ErrCode: 10167, Description: "too many requests"},
		JSStreamTransformInvalidDestination:          {Code: 400, ErrCode: 10156, Description: "stream transform: {err}"},
		JSStreamTransformInvalidSource:               {Code: 400, ErrCode: 10155, Description: "stream transform source: {err}"},
		JSStreamUpdateErrF:                           {Code: 500, ErrCode: 10069, Description: "{err}"},
		JSStreamWrongLastMsgIDErrF:                   {Code: 400, ErrCode: 10070, Description: "wrong last msg ID: {id}"},
		JSStreamWrongLastSequenceConstantErr:         {Code: 400, ErrCode: 10164, Description: "wrong last sequence"},
		JSStreamWrongLastSequenceErrF:                {Code: 400, ErrCode: 10071, Description: "wrong last sequence: {seq}"},
		JSTempStorageFailedErr:                       {Code: 500, ErrCode: 10072, Description: "JetStream unable to open temp storage for restore"},
		JSTemplateNameNotMatchSubjectErr:             {Code: 400, ErrCode: 10073, Description: "template name in subject does not match request"},
	}
	// ErrJetStreamNotClustered Deprecated by JSClusterNotActiveErr ApiError, use IsNatsError() for comparisons
	ErrJetStreamNotClustered = ApiErrors[JSClusterNotActiveErr]
	// ErrJetStreamNotAssigned Deprecated by JSClusterNotAssignedErr ApiError, use IsNatsError() for comparisons
	ErrJetStreamNotAssigned = ApiErrors[JSClusterNotAssignedErr]
	// ErrJetStreamNotLeader Deprecated by JSClusterNotLeaderErr ApiError, use IsNatsError() for comparisons
	ErrJetStreamNotLeader = ApiErrors[JSClusterNotLeaderErr]
	// ErrJetStreamConsumerAlreadyUsed Deprecated by JSConsumerNameExistErr ApiError, use IsNatsError() for comparisons
	ErrJetStreamConsumerAlreadyUsed = ApiErrors[JSConsumerNameExistErr]
	// ErrJetStreamResourcesExceeded Deprecated by JSInsufficientResourcesErr ApiError, use IsNatsError() for comparisons
	ErrJetStreamResourcesExceeded = ApiErrors[JSInsufficientResourcesErr]
	// ErrMemoryResourcesExceeded Deprecated by JSMemoryResourcesExceededErr ApiError, use IsNatsError() for comparisons
	ErrMemoryResourcesExceeded = ApiErrors[JSMemoryResourcesExceededErr]
	// ErrJetStreamNotEnabled Deprecated by JSNotEnabledErr ApiError, use IsNatsError() for comparisons
	ErrJetStreamNotEnabled = ApiErrors[JSNotEnabledErr]
	// ErrStorageResourcesExceeded Deprecated by JSStorageResourcesExceededErr ApiError, use IsNatsError() for comparisons
	ErrStorageResourcesExceeded = ApiErrors[JSStorageResourcesExceededErr]
	// ErrJetStreamStreamAlreadyUsed Deprecated by JSStreamNameExistErr ApiError, use IsNatsError() for comparisons
	ErrJetStreamStreamAlreadyUsed = ApiErrors[JSStreamNameExistErr]
	// ErrJetStreamStreamNotFound Deprecated by JSStreamNotFoundErr ApiError, use IsNatsError() for comparisons
	ErrJetStreamStreamNotFound = ApiErrors[JSStreamNotFoundErr]
	// ErrReplicasNotSupported Deprecated by JSStreamReplicasNotSupportedErr ApiError, use IsNatsError() for comparisons
	ErrReplicasNotSupported = ApiErrors[JSStreamReplicasNotSupportedErr]
)

// NewJSAccountResourcesExceededError creates a new JSAccountResourcesExceededErr error: "resource limits exceeded for account"
func NewJSAccountResourcesExceededError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSAccountResourcesExceededErr]
}

// NewJSAtomicPublishDisabledError creates a new JSAtomicPublishDisabledErr error: "atomic publish is disabled"
func NewJSAtomicPublishDisabledError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSAtomicPublishDisabledErr]
}

// NewJSAtomicPublishIncompleteBatchError creates a new JSAtomicPublishIncompleteBatchErr error: "atomic publish batch is incomplete"
func NewJSAtomicPublishIncompleteBatchError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSAtomicPublishIncompleteBatchErr]
}

// NewJSAtomicPublishInvalidBatchIDError creates a new JSAtomicPublishInvalidBatchIDErr error: "atomic publish batch ID is invalid"
func NewJSAtomicPublishInvalidBatchIDError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSAtomicPublishInvalidBatchIDErr]
}

// NewJSAtomicPublishMissingSeqError creates a new JSAtomicPublishMissingSeqErr error: "atomic publish sequence is missing"
func NewJSAtomicPublishMissingSeqError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSAtomicPublishMissingSeqErr]
}

// NewJSAtomicPublishTooLargeBatchError creates a new JSAtomicPublishTooLargeBatchErr error: "atomic publish batch is too large"
func NewJSAtomicPublishTooLargeBatchError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSAtomicPublishTooLargeBatchErr]
}

// NewJSAtomicPublishUnsupportedHeaderBatchError creates a new JSAtomicPublishUnsupportedHeaderBatchErr error: "atomic publish unsupported header used: {header}"
func NewJSAtomicPublishUnsupportedHeaderBatchError(header interface{}, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSAtomicPublishUnsupportedHeaderBatchErr]
	args := e.toReplacerArgs([]interface{}{"{header}", header})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSBadRequestError creates a new JSBadRequestErr error: "bad request"
func NewJSBadRequestError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSBadRequestErr]
}

// NewJSClusterIncompleteError creates a new JSClusterIncompleteErr error: "incomplete results"
func NewJSClusterIncompleteError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSClusterIncompleteErr]
}

// NewJSClusterNoPeersError creates a new JSClusterNoPeersErrF error: "{err}"
func NewJSClusterNoPeersError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSClusterNoPeersErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSClusterNotActiveError creates a new JSClusterNotActiveErr error: "JetStream not in clustered mode"
func NewJSClusterNotActiveError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSClusterNotActiveErr]
}

// NewJSClusterNotAssignedError creates a new JSClusterNotAssignedErr error: "JetStream cluster not assigned to this server"
func NewJSClusterNotAssignedError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSClusterNotAssignedErr]
}

// NewJSClusterNotAvailError creates a new JSClusterNotAvailErr error: "JetStream system temporarily unavailable"
func NewJSClusterNotAvailError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSClusterNotAvailErr]
}

// NewJSClusterNotLeaderError creates a new JSClusterNotLeaderErr error: "JetStream cluster can not handle request"
func NewJSClusterNotLeaderError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSClusterNotLeaderErr]
}

// NewJSClusterPeerNotMemberError creates a new JSClusterPeerNotMemberErr error: "peer not a member"
func NewJSClusterPeerNotMemberError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSClusterPeerNotMemberErr]
}

// NewJSClusterRequiredError creates a new JSClusterRequiredErr error: "JetStream clustering support required"
func NewJSClusterRequiredError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSClusterRequiredErr]
}

// NewJSClusterServerNotMemberError creates a new JSClusterServerNotMemberErr error: "server is not a member of the cluster"
func NewJSClusterServerNotMemberError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSClusterServerNotMemberErr]
}

// NewJSClusterTagsError creates a new JSClusterTagsErr error: "tags placement not supported for operation"
func NewJSClusterTagsError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSClusterTagsErr]
}

// NewJSClusterUnSupportFeatureError creates a new JSClusterUnSupportFeatureErr error: "not currently supported in clustered mode"
func NewJSClusterUnSupportFeatureError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSClusterUnSupportFeatureErr]
}

// NewJSConsumerAckPolicyInvalidError creates a new JSConsumerAckPolicyInvalidErr error: "consumer ack policy invalid"
func NewJSConsumerAckPolicyInvalidError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerAckPolicyInvalidErr]
}

// NewJSConsumerAckWaitNegativeError creates a new JSConsumerAckWaitNegativeErr error: "consumer ack wait needs to be positive"
func NewJSConsumerAckWaitNegativeError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerAckWaitNegativeErr]
}

// NewJSConsumerAlreadyExistsError creates a new JSConsumerAlreadyExists error: "consumer already exists"
func NewJSConsumerAlreadyExistsError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerAlreadyExists]
}

// NewJSConsumerBackOffNegativeError creates a new JSConsumerBackOffNegativeErr error: "consumer backoff needs to be positive"
func NewJSConsumerBackOffNegativeError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerBackOffNegativeErr]
}

// NewJSConsumerBadDurableNameError creates a new JSConsumerBadDurableNameErr error: "durable name can not contain '.', '*', '>'"
func NewJSConsumerBadDurableNameError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerBadDurableNameErr]
}

// NewJSConsumerConfigRequiredError creates a new JSConsumerConfigRequiredErr error: "consumer config required"
func NewJSConsumerConfigRequiredError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerConfigRequiredErr]
}

// NewJSConsumerCreateDurableAndNameMismatchError creates a new JSConsumerCreateDurableAndNameMismatch error: "Consumer Durable and Name have to be equal if both are provided"
func NewJSConsumerCreateDurableAndNameMismatchError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerCreateDurableAndNameMismatch]
}

// NewJSConsumerCreateError creates a new JSConsumerCreateErrF error: "{err}"
func NewJSConsumerCreateError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSConsumerCreateErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSConsumerCreateFilterSubjectMismatchError creates a new JSConsumerCreateFilterSubjectMismatchErr error: "Consumer create request did not match filtered subject from create subject"
func NewJSConsumerCreateFilterSubjectMismatchError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerCreateFilterSubjectMismatchErr]
}

// NewJSConsumerDeliverCycleError creates a new JSConsumerDeliverCycleErr error: "consumer deliver subject forms a cycle"
func NewJSConsumerDeliverCycleError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerDeliverCycleErr]
}

// NewJSConsumerDeliverToWildcardsError creates a new JSConsumerDeliverToWildcardsErr error: "consumer deliver subject has wildcards"
func NewJSConsumerDeliverToWildcardsError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerDeliverToWildcardsErr]
}

// NewJSConsumerDescriptionTooLongError creates a new JSConsumerDescriptionTooLongErrF error: "consumer description is too long, maximum allowed is {max}"
func NewJSConsumerDescriptionTooLongError(max interface{}, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSConsumerDescriptionTooLongErrF]
	args := e.toReplacerArgs([]interface{}{"{max}", max})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSConsumerDirectRequiresEphemeralError creates a new JSConsumerDirectRequiresEphemeralErr error: "consumer direct requires an ephemeral consumer"
func NewJSConsumerDirectRequiresEphemeralError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerDirectRequiresEphemeralErr]
}

// NewJSConsumerDirectRequiresPushError creates a new JSConsumerDirectRequiresPushErr error: "consumer direct requires a push based consumer"
func NewJSConsumerDirectRequiresPushError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerDirectRequiresPushErr]
}

// NewJSConsumerDoesNotExistError creates a new JSConsumerDoesNotExist error: "consumer does not exist"
func NewJSConsumerDoesNotExistError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerDoesNotExist]
}

// NewJSConsumerDuplicateFilterSubjectsError creates a new JSConsumerDuplicateFilterSubjects error: "consumer cannot have both FilterSubject and FilterSubjects specified"
func NewJSConsumerDuplicateFilterSubjectsError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerDuplicateFilterSubjects]
}

// NewJSConsumerDurableNameNotInSubjectError creates a new JSConsumerDurableNameNotInSubjectErr error: "consumer expected to be durable but no durable name set in subject"
func NewJSConsumerDurableNameNotInSubjectError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerDurableNameNotInSubjectErr]
}

// NewJSConsumerDurableNameNotMatchSubjectError creates a new JSConsumerDurableNameNotMatchSubjectErr error: "consumer name in subject does not match durable name in request"
func NewJSConsumerDurableNameNotMatchSubjectError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerDurableNameNotMatchSubjectErr]
}

// NewJSConsumerDurableNameNotSetError creates a new JSConsumerDurableNameNotSetErr error: "consumer expected to be durable but a durable name was not set"
func NewJSConsumerDurableNameNotSetError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerDurableNameNotSetErr]
}

// NewJSConsumerEmptyFilterError creates a new JSConsumerEmptyFilter error: "consumer filter in FilterSubjects cannot be empty"
func NewJSConsumerEmptyFilterError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerEmptyFilter]
}

// NewJSConsumerEmptyGroupNameError creates a new JSConsumerEmptyGroupName error: "Group name cannot be an empty string"
func NewJSConsumerEmptyGroupNameError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerEmptyGroupName]
}

// NewJSConsumerEphemeralWithDurableInSubjectError creates a new JSConsumerEphemeralWithDurableInSubjectErr error: "consumer expected to be ephemeral but detected a durable name set in subject"
func NewJSConsumerEphemeralWithDurableInSubjectError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerEphemeralWithDurableInSubjectErr]
}

// NewJSConsumerEphemeralWithDurableNameError creates a new JSConsumerEphemeralWithDurableNameErr error: "consumer expected to be ephemeral but a durable name was set in request"
func NewJSConsumerEphemeralWithDurableNameError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerEphemeralWithDurableNameErr]
}

// NewJSConsumerExistingActiveError creates a new JSConsumerExistingActiveErr error: "consumer already exists and is still active"
func NewJSConsumerExistingActiveError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerExistingActiveErr]
}

// NewJSConsumerFCRequiresPushError creates a new JSConsumerFCRequiresPushErr error: "consumer flow control requires a push based consumer"
func NewJSConsumerFCRequiresPushError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerFCRequiresPushErr]
}

// NewJSConsumerFilterNotSubsetError creates a new JSConsumerFilterNotSubsetErr error: "consumer filter subject is not a valid subset of the interest subjects"
func NewJSConsumerFilterNotSubsetError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerFilterNotSubsetErr]
}

// NewJSConsumerHBRequiresPushError creates a new JSConsumerHBRequiresPushErr error: "consumer idle heartbeat requires a push based consumer"
func NewJSConsumerHBRequiresPushError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerHBRequiresPushErr]
}

// NewJSConsumerInactiveThresholdExcessError creates a new JSConsumerInactiveThresholdExcess error: "consumer inactive threshold exceeds system limit of {limit}"
func NewJSConsumerInactiveThresholdExcessError(limit interface{}, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSConsumerInactiveThresholdExcess]
	args := e.toReplacerArgs([]interface{}{"{limit}", limit})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSConsumerInvalidDeliverSubjectError creates a new JSConsumerInvalidDeliverSubject error: "invalid push consumer deliver subject"
func NewJSConsumerInvalidDeliverSubjectError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerInvalidDeliverSubject]
}

// NewJSConsumerInvalidGroupNameError creates a new JSConsumerInvalidGroupNameErr error: "Valid priority group name must match A-Z, a-z, 0-9, -_/=)+ and may not exceed 16 characters"
func NewJSConsumerInvalidGroupNameError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerInvalidGroupNameErr]
}

// NewJSConsumerInvalidPolicyError creates a new JSConsumerInvalidPolicyErrF error: "{err}"
func NewJSConsumerInvalidPolicyError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSConsumerInvalidPolicyErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSConsumerInvalidPriorityGroupError creates a new JSConsumerInvalidPriorityGroupErr error: "Provided priority group does not exist for this consumer"
func NewJSConsumerInvalidPriorityGroupError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerInvalidPriorityGroupErr]
}

// NewJSConsumerInvalidSamplingError creates a new JSConsumerInvalidSamplingErrF error: "failed to parse consumer sampling configuration: {err}"
func NewJSConsumerInvalidSamplingError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSConsumerInvalidSamplingErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSConsumerMaxDeliverBackoffError creates a new JSConsumerMaxDeliverBackoffErr error: "max deliver is required to be > length of backoff values"
func NewJSConsumerMaxDeliverBackoffError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerMaxDeliverBackoffErr]
}

// NewJSConsumerMaxPendingAckExcessError creates a new JSConsumerMaxPendingAckExcessErrF error: "consumer max ack pending exceeds system limit of {limit}"
func NewJSConsumerMaxPendingAckExcessError(limit interface{}, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSConsumerMaxPendingAckExcessErrF]
	args := e.toReplacerArgs([]interface{}{"{limit}", limit})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSConsumerMaxPendingAckPolicyRequiredError creates a new JSConsumerMaxPendingAckPolicyRequiredErr error: "consumer requires ack policy for max ack pending"
func NewJSConsumerMaxPendingAckPolicyRequiredError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerMaxPendingAckPolicyRequiredErr]
}

// NewJSConsumerMaxRequestBatchExceededError creates a new JSConsumerMaxRequestBatchExceededF error: "consumer max request batch exceeds server limit of {limit}"
func NewJSConsumerMaxRequestBatchExceededError(limit interface{}, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSConsumerMaxRequestBatchExceededF]
	args := e.toReplacerArgs([]interface{}{"{limit}", limit})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSConsumerMaxRequestBatchNegativeError creates a new JSConsumerMaxRequestBatchNegativeErr error: "consumer max request batch needs to be > 0"
func NewJSConsumerMaxRequestBatchNegativeError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerMaxRequestBatchNegativeErr]
}

// NewJSConsumerMaxRequestExpiresTooSmallError creates a new JSConsumerMaxRequestExpiresTooSmall error: "consumer max request expires needs to be >= 1ms"
func NewJSConsumerMaxRequestExpiresTooSmallError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerMaxRequestExpiresTooSmall]
}

// NewJSConsumerMaxWaitingNegativeError creates a new JSConsumerMaxWaitingNegativeErr error: "consumer max waiting needs to be positive"
func NewJSConsumerMaxWaitingNegativeError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerMaxWaitingNegativeErr]
}

// NewJSConsumerMetadataLengthError creates a new JSConsumerMetadataLengthErrF error: "consumer metadata exceeds maximum size of {limit}"
func NewJSConsumerMetadataLengthError(limit interface{}, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSConsumerMetadataLengthErrF]
	args := e.toReplacerArgs([]interface{}{"{limit}", limit})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSConsumerMultipleFiltersNotAllowedError creates a new JSConsumerMultipleFiltersNotAllowed error: "consumer with multiple subject filters cannot use subject based API"
func NewJSConsumerMultipleFiltersNotAllowedError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerMultipleFiltersNotAllowed]
}

// NewJSConsumerNameContainsPathSeparatorsError creates a new JSConsumerNameContainsPathSeparatorsErr error: "Consumer name can not contain path separators"
func NewJSConsumerNameContainsPathSeparatorsError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerNameContainsPathSeparatorsErr]
}

// NewJSConsumerNameExistError creates a new JSConsumerNameExistErr error: "consumer name already in use"
func NewJSConsumerNameExistError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerNameExistErr]
}

// NewJSConsumerNameTooLongError creates a new JSConsumerNameTooLongErrF error: "consumer name is too long, maximum allowed is {max}"
func NewJSConsumerNameTooLongError(max interface{}, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSConsumerNameTooLongErrF]
	args := e.toReplacerArgs([]interface{}{"{max}", max})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSConsumerNotFoundError creates a new JSConsumerNotFoundErr error: "consumer not found"
func NewJSConsumerNotFoundError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerNotFoundErr]
}

// NewJSConsumerOfflineError creates a new JSConsumerOfflineErr error: "consumer is offline"
func NewJSConsumerOfflineError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerOfflineErr]
}

// NewJSConsumerOfflineReasonError creates a new JSConsumerOfflineReasonErrF error: "consumer is offline: {err}"
func NewJSConsumerOfflineReasonError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSConsumerOfflineReasonErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSConsumerOnMappedError creates a new JSConsumerOnMappedErr error: "consumer direct on a mapped consumer"
func NewJSConsumerOnMappedError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerOnMappedErr]
}

// NewJSConsumerOverlappingSubjectFiltersError creates a new JSConsumerOverlappingSubjectFilters error: "consumer subject filters cannot overlap"
func NewJSConsumerOverlappingSubjectFiltersError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerOverlappingSubjectFilters]
}

// NewJSConsumerPinnedTTLWithoutPriorityPolicyNoneError creates a new JSConsumerPinnedTTLWithoutPriorityPolicyNone error: "PinnedTTL cannot be set when PriorityPolicy is none"
func NewJSConsumerPinnedTTLWithoutPriorityPolicyNoneError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerPinnedTTLWithoutPriorityPolicyNone]
}

// NewJSConsumerPriorityGroupWithPolicyNoneError creates a new JSConsumerPriorityGroupWithPolicyNone error: "consumer can not have priority groups when policy is none"
func NewJSConsumerPriorityGroupWithPolicyNoneError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerPriorityGroupWithPolicyNone]
}

// NewJSConsumerPriorityPolicyWithoutGroupError creates a new JSConsumerPriorityPolicyWithoutGroup error: "Setting PriorityPolicy requires at least one PriorityGroup to be set"
func NewJSConsumerPriorityPolicyWithoutGroupError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerPriorityPolicyWithoutGroup]
}

// NewJSConsumerPullNotDurableError creates a new JSConsumerPullNotDurableErr error: "consumer in pull mode requires a durable name"
func NewJSConsumerPullNotDurableError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerPullNotDurableErr]
}

// NewJSConsumerPullRequiresAckError creates a new JSConsumerPullRequiresAckErr error: "consumer in pull mode requires explicit ack policy on workqueue stream"
func NewJSConsumerPullRequiresAckError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerPullRequiresAckErr]
}

// NewJSConsumerPullWithRateLimitError creates a new JSConsumerPullWithRateLimitErr error: "consumer in pull mode can not have rate limit set"
func NewJSConsumerPullWithRateLimitError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerPullWithRateLimitErr]
}

// NewJSConsumerPushMaxWaitingError creates a new JSConsumerPushMaxWaitingErr error: "consumer in push mode can not set max waiting"
func NewJSConsumerPushMaxWaitingError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerPushMaxWaitingErr]
}

// NewJSConsumerPushWithPriorityGroupError creates a new JSConsumerPushWithPriorityGroupErr error: "priority groups can not be used with push consumers"
func NewJSConsumerPushWithPriorityGroupError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerPushWithPriorityGroupErr]
}

// NewJSConsumerReplacementWithDifferentNameError creates a new JSConsumerReplacementWithDifferentNameErr error: "consumer replacement durable config not the same"
func NewJSConsumerReplacementWithDifferentNameError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerReplacementWithDifferentNameErr]
}

// NewJSConsumerReplayPolicyInvalidError creates a new JSConsumerReplayPolicyInvalidErr error: "consumer replay policy invalid"
func NewJSConsumerReplayPolicyInvalidError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerReplayPolicyInvalidErr]
}

// NewJSConsumerReplicasExceedsStreamError creates a new JSConsumerReplicasExceedsStream error: "consumer config replica count exceeds parent stream"
func NewJSConsumerReplicasExceedsStreamError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerReplicasExceedsStream]
}

// NewJSConsumerReplicasShouldMatchStreamError creates a new JSConsumerReplicasShouldMatchStream error: "consumer config replicas must match interest retention stream's replicas"
func NewJSConsumerReplicasShouldMatchStreamError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerReplicasShouldMatchStream]
}

// NewJSConsumerSmallHeartbeatError creates a new JSConsumerSmallHeartbeatErr error: "consumer idle heartbeat needs to be >= 100ms"
func NewJSConsumerSmallHeartbeatError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerSmallHeartbeatErr]
}

// NewJSConsumerStoreFailedError creates a new JSConsumerStoreFailedErrF error: "error creating store for consumer: {err}"
func NewJSConsumerStoreFailedError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSConsumerStoreFailedErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSConsumerWQConsumerNotDeliverAllError creates a new JSConsumerWQConsumerNotDeliverAllErr error: "consumer must be deliver all on workqueue stream"
func NewJSConsumerWQConsumerNotDeliverAllError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerWQConsumerNotDeliverAllErr]
}

// NewJSConsumerWQConsumerNotUniqueError creates a new JSConsumerWQConsumerNotUniqueErr error: "filtered consumer not unique on workqueue stream"
func NewJSConsumerWQConsumerNotUniqueError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerWQConsumerNotUniqueErr]
}

// NewJSConsumerWQMultipleUnfilteredError creates a new JSConsumerWQMultipleUnfilteredErr error: "multiple non-filtered consumers not allowed on workqueue stream"
func NewJSConsumerWQMultipleUnfilteredError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerWQMultipleUnfilteredErr]
}

// NewJSConsumerWQRequiresExplicitAckError creates a new JSConsumerWQRequiresExplicitAckErr error: "workqueue stream requires explicit ack"
func NewJSConsumerWQRequiresExplicitAckError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerWQRequiresExplicitAckErr]
}

// NewJSConsumerWithFlowControlNeedsHeartbeatsError creates a new JSConsumerWithFlowControlNeedsHeartbeats error: "consumer with flow control also needs heartbeats"
func NewJSConsumerWithFlowControlNeedsHeartbeatsError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSConsumerWithFlowControlNeedsHeartbeats]
}

// NewJSInsufficientResourcesError creates a new JSInsufficientResourcesErr error: "insufficient resources"
func NewJSInsufficientResourcesError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSInsufficientResourcesErr]
}

// NewJSInvalidJSONError creates a new JSInvalidJSONErr error: "invalid JSON: {err}"
func NewJSInvalidJSONError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSInvalidJSONErr]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSMaximumConsumersLimitError creates a new JSMaximumConsumersLimitErr error: "maximum consumers limit reached"
func NewJSMaximumConsumersLimitError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMaximumConsumersLimitErr]
}

// NewJSMaximumStreamsLimitError creates a new JSMaximumStreamsLimitErr error: "maximum number of streams reached"
func NewJSMaximumStreamsLimitError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMaximumStreamsLimitErr]
}

// NewJSMemoryResourcesExceededError creates a new JSMemoryResourcesExceededErr error: "insufficient memory resources available"
func NewJSMemoryResourcesExceededError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMemoryResourcesExceededErr]
}

// NewJSMessageCounterBrokenError creates a new JSMessageCounterBrokenErr error: "message counter is broken"
func NewJSMessageCounterBrokenError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMessageCounterBrokenErr]
}

// NewJSMessageIncrDisabledError creates a new JSMessageIncrDisabledErr error: "message counters is disabled"
func NewJSMessageIncrDisabledError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMessageIncrDisabledErr]
}

// NewJSMessageIncrInvalidError creates a new JSMessageIncrInvalidErr error: "message counter increment is invalid"
func NewJSMessageIncrInvalidError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMessageIncrInvalidErr]
}

// NewJSMessageIncrMissingError creates a new JSMessageIncrMissingErr error: "message counter increment is missing"
func NewJSMessageIncrMissingError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMessageIncrMissingErr]
}

// NewJSMessageIncrPayloadError creates a new JSMessageIncrPayloadErr error: "message counter has payload"
func NewJSMessageIncrPayloadError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMessageIncrPayloadErr]
}

// NewJSMessageSchedulesDisabledError creates a new JSMessageSchedulesDisabledErr error: "message schedules is disabled"
func NewJSMessageSchedulesDisabledError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMessageSchedulesDisabledErr]
}

// NewJSMessageSchedulesPatternInvalidError creates a new JSMessageSchedulesPatternInvalidErr error: "message schedules pattern is invalid"
func NewJSMessageSchedulesPatternInvalidError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMessageSchedulesPatternInvalidErr]
}

// NewJSMessageSchedulesRollupInvalidError creates a new JSMessageSchedulesRollupInvalidErr error: "message schedules invalid rollup"
func NewJSMessageSchedulesRollupInvalidError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMessageSchedulesRollupInvalidErr]
}

// NewJSMessageSchedulesTTLInvalidError creates a new JSMessageSchedulesTTLInvalidErr error: "message schedules invalid per-message TTL"
func NewJSMessageSchedulesTTLInvalidError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMessageSchedulesTTLInvalidErr]
}

// NewJSMessageSchedulesTargetInvalidError creates a new JSMessageSchedulesTargetInvalidErr error: "message schedules target is invalid"
func NewJSMessageSchedulesTargetInvalidError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMessageSchedulesTargetInvalidErr]
}

// NewJSMessageTTLDisabledError creates a new JSMessageTTLDisabledErr error: "per-message TTL is disabled"
func NewJSMessageTTLDisabledError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMessageTTLDisabledErr]
}

// NewJSMessageTTLInvalidError creates a new JSMessageTTLInvalidErr error: "invalid per-message TTL"
func NewJSMessageTTLInvalidError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMessageTTLInvalidErr]
}

// NewJSMirrorConsumerSetupFailedError creates a new JSMirrorConsumerSetupFailedErrF error: "{err}"
func NewJSMirrorConsumerSetupFailedError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSMirrorConsumerSetupFailedErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSMirrorInvalidStreamNameError creates a new JSMirrorInvalidStreamName error: "mirrored stream name is invalid"
func NewJSMirrorInvalidStreamNameError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMirrorInvalidStreamName]
}

// NewJSMirrorInvalidSubjectFilterError creates a new JSMirrorInvalidSubjectFilter error: "mirror transform source: {err}"
func NewJSMirrorInvalidSubjectFilterError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSMirrorInvalidSubjectFilter]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSMirrorInvalidTransformDestinationError creates a new JSMirrorInvalidTransformDestination error: "mirror transform: {err}"
func NewJSMirrorInvalidTransformDestinationError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSMirrorInvalidTransformDestination]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSMirrorMaxMessageSizeTooBigError creates a new JSMirrorMaxMessageSizeTooBigErr error: "stream mirror must have max message size >= source"
func NewJSMirrorMaxMessageSizeTooBigError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMirrorMaxMessageSizeTooBigErr]
}

// NewJSMirrorMultipleFiltersNotAllowedError creates a new JSMirrorMultipleFiltersNotAllowed error: "mirror with multiple subject transforms cannot also have a single subject filter"
func NewJSMirrorMultipleFiltersNotAllowedError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMirrorMultipleFiltersNotAllowed]
}

// NewJSMirrorOverlappingSubjectFiltersError creates a new JSMirrorOverlappingSubjectFilters error: "mirror subject filters can not overlap"
func NewJSMirrorOverlappingSubjectFiltersError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMirrorOverlappingSubjectFilters]
}

// NewJSMirrorWithAtomicPublishError creates a new JSMirrorWithAtomicPublishErr error: "stream mirrors can not also use atomic publishing"
func NewJSMirrorWithAtomicPublishError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMirrorWithAtomicPublishErr]
}

// NewJSMirrorWithCountersError creates a new JSMirrorWithCountersErr error: "stream mirrors can not also calculate counters"
func NewJSMirrorWithCountersError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMirrorWithCountersErr]
}

// NewJSMirrorWithFirstSeqError creates a new JSMirrorWithFirstSeqErr error: "stream mirrors can not have first sequence configured"
func NewJSMirrorWithFirstSeqError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMirrorWithFirstSeqErr]
}

// NewJSMirrorWithMsgSchedulesError creates a new JSMirrorWithMsgSchedulesErr error: "stream mirrors can not also schedule messages"
func NewJSMirrorWithMsgSchedulesError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMirrorWithMsgSchedulesErr]
}

// NewJSMirrorWithSourcesError creates a new JSMirrorWithSourcesErr error: "stream mirrors can not also contain other sources"
func NewJSMirrorWithSourcesError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMirrorWithSourcesErr]
}

// NewJSMirrorWithStartSeqAndTimeError creates a new JSMirrorWithStartSeqAndTimeErr error: "stream mirrors can not have both start seq and start time configured"
func NewJSMirrorWithStartSeqAndTimeError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMirrorWithStartSeqAndTimeErr]
}

// NewJSMirrorWithSubjectFiltersError creates a new JSMirrorWithSubjectFiltersErr error: "stream mirrors can not contain filtered subjects"
func NewJSMirrorWithSubjectFiltersError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMirrorWithSubjectFiltersErr]
}

// NewJSMirrorWithSubjectsError creates a new JSMirrorWithSubjectsErr error: "stream mirrors can not contain subjects"
func NewJSMirrorWithSubjectsError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSMirrorWithSubjectsErr]
}

// NewJSNoAccountError creates a new JSNoAccountErr error: "account not found"
func NewJSNoAccountError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSNoAccountErr]
}

// NewJSNoLimitsError creates a new JSNoLimitsErr error: "no JetStream default or applicable tiered limit present"
func NewJSNoLimitsError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSNoLimitsErr]
}

// NewJSNoMessageFoundError creates a new JSNoMessageFoundErr error: "no message found"
func NewJSNoMessageFoundError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSNoMessageFoundErr]
}

// NewJSNotEmptyRequestError creates a new JSNotEmptyRequestErr error: "expected an empty request payload"
func NewJSNotEmptyRequestError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSNotEmptyRequestErr]
}

// NewJSNotEnabledError creates a new JSNotEnabledErr error: "JetStream not enabled"
func NewJSNotEnabledError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSNotEnabledErr]
}

// NewJSNotEnabledForAccountError creates a new JSNotEnabledForAccountErr error: "JetStream not enabled for account"
func NewJSNotEnabledForAccountError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSNotEnabledForAccountErr]
}

// NewJSPedanticError creates a new JSPedanticErrF error: "pedantic mode: {err}"
func NewJSPedanticError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSPedanticErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSPeerRemapError creates a new JSPeerRemapErr error: "peer remap failed"
func NewJSPeerRemapError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSPeerRemapErr]
}

// NewJSRaftGeneralError creates a new JSRaftGeneralErrF error: "{err}"
func NewJSRaftGeneralError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSRaftGeneralErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSReplicasCountCannotBeNegativeError creates a new JSReplicasCountCannotBeNegative error: "replicas count cannot be negative"
func NewJSReplicasCountCannotBeNegativeError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSReplicasCountCannotBeNegative]
}

// NewJSRequiredApiLevelError creates a new JSRequiredApiLevelErr error: "JetStream minimum api level required"
func NewJSRequiredApiLevelError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSRequiredApiLevelErr]
}

// NewJSRestoreSubscribeFailedError creates a new JSRestoreSubscribeFailedErrF error: "JetStream unable to subscribe to restore snapshot {subject}: {err}"
func NewJSRestoreSubscribeFailedError(err error, subject interface{}, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSRestoreSubscribeFailedErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err, "{subject}", subject})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSSequenceNotFoundError creates a new JSSequenceNotFoundErrF error: "sequence {seq} not found"
func NewJSSequenceNotFoundError(seq uint64, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSSequenceNotFoundErrF]
	args := e.toReplacerArgs([]interface{}{"{seq}", seq})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSSnapshotDeliverSubjectInvalidError creates a new JSSnapshotDeliverSubjectInvalidErr error: "deliver subject not valid"
func NewJSSnapshotDeliverSubjectInvalidError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSSnapshotDeliverSubjectInvalidErr]
}

// NewJSSourceConsumerSetupFailedError creates a new JSSourceConsumerSetupFailedErrF error: "{err}"
func NewJSSourceConsumerSetupFailedError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSSourceConsumerSetupFailedErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSSourceDuplicateDetectedError creates a new JSSourceDuplicateDetected error: "duplicate source configuration detected"
func NewJSSourceDuplicateDetectedError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSSourceDuplicateDetected]
}

// NewJSSourceInvalidStreamNameError creates a new JSSourceInvalidStreamName error: "sourced stream name is invalid"
func NewJSSourceInvalidStreamNameError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSSourceInvalidStreamName]
}

// NewJSSourceInvalidSubjectFilterError creates a new JSSourceInvalidSubjectFilter error: "source transform source: {err}"
func NewJSSourceInvalidSubjectFilterError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSSourceInvalidSubjectFilter]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSSourceInvalidTransformDestinationError creates a new JSSourceInvalidTransformDestination error: "source transform: {err}"
func NewJSSourceInvalidTransformDestinationError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSSourceInvalidTransformDestination]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSSourceMaxMessageSizeTooBigError creates a new JSSourceMaxMessageSizeTooBigErr error: "stream source must have max message size >= target"
func NewJSSourceMaxMessageSizeTooBigError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSSourceMaxMessageSizeTooBigErr]
}

// NewJSSourceMultipleFiltersNotAllowedError creates a new JSSourceMultipleFiltersNotAllowed error: "source with multiple subject transforms cannot also have a single subject filter"
func NewJSSourceMultipleFiltersNotAllowedError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSSourceMultipleFiltersNotAllowed]
}

// NewJSSourceOverlappingSubjectFiltersError creates a new JSSourceOverlappingSubjectFilters error: "source filters can not overlap"
func NewJSSourceOverlappingSubjectFiltersError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSSourceOverlappingSubjectFilters]
}

// NewJSSourceWithMsgSchedulesError creates a new JSSourceWithMsgSchedulesErr error: "stream source can not also schedule messages"
func NewJSSourceWithMsgSchedulesError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSSourceWithMsgSchedulesErr]
}

// NewJSStorageResourcesExceededError creates a new JSStorageResourcesExceededErr error: "insufficient storage resources available"
func NewJSStorageResourcesExceededError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStorageResourcesExceededErr]
}

// NewJSStreamAssignmentError creates a new JSStreamAssignmentErrF error: "{err}"
func NewJSStreamAssignmentError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamAssignmentErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamCreateError creates a new JSStreamCreateErrF error: "{err}"
func NewJSStreamCreateError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamCreateErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamDeleteError creates a new JSStreamDeleteErrF error: "{err}"
func NewJSStreamDeleteError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamDeleteErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamDuplicateMessageConflictError creates a new JSStreamDuplicateMessageConflict error: "duplicate message id is in process"
func NewJSStreamDuplicateMessageConflictError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamDuplicateMessageConflict]
}

// NewJSStreamExpectedLastSeqPerSubjectInvalidError creates a new JSStreamExpectedLastSeqPerSubjectInvalid error: "missing sequence for expected last sequence per subject"
func NewJSStreamExpectedLastSeqPerSubjectInvalidError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamExpectedLastSeqPerSubjectInvalid]
}

// NewJSStreamExpectedLastSeqPerSubjectNotReadyError creates a new JSStreamExpectedLastSeqPerSubjectNotReady error: "expected last sequence per subject temporarily unavailable"
func NewJSStreamExpectedLastSeqPerSubjectNotReadyError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamExpectedLastSeqPerSubjectNotReady]
}

// NewJSStreamExternalApiOverlapError creates a new JSStreamExternalApiOverlapErrF error: "stream external api prefix {prefix} must not overlap with {subject}"
func NewJSStreamExternalApiOverlapError(prefix interface{}, subject interface{}, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamExternalApiOverlapErrF]
	args := e.toReplacerArgs([]interface{}{"{prefix}", prefix, "{subject}", subject})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamExternalDelPrefixOverlapsError creates a new JSStreamExternalDelPrefixOverlapsErrF error: "stream external delivery prefix {prefix} overlaps with stream subject {subject}"
func NewJSStreamExternalDelPrefixOverlapsError(prefix interface{}, subject interface{}, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamExternalDelPrefixOverlapsErrF]
	args := e.toReplacerArgs([]interface{}{"{prefix}", prefix, "{subject}", subject})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamGeneralError creates a new JSStreamGeneralErrorF error: "{err}"
func NewJSStreamGeneralError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamGeneralErrorF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamHeaderExceedsMaximumError creates a new JSStreamHeaderExceedsMaximumErr error: "header size exceeds maximum allowed of 64k"
func NewJSStreamHeaderExceedsMaximumError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamHeaderExceedsMaximumErr]
}

// NewJSStreamInfoMaxSubjectsError creates a new JSStreamInfoMaxSubjectsErr error: "subject details would exceed maximum allowed"
func NewJSStreamInfoMaxSubjectsError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamInfoMaxSubjectsErr]
}

// NewJSStreamInvalidConfigError creates a new JSStreamInvalidConfigF error: "{err}"
func NewJSStreamInvalidConfigError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamInvalidConfigF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamInvalidError creates a new JSStreamInvalidErr error: "stream not valid"
func NewJSStreamInvalidError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamInvalidErr]
}

// NewJSStreamInvalidExternalDeliverySubjError creates a new JSStreamInvalidExternalDeliverySubjErrF error: "stream external delivery prefix {prefix} must not contain wildcards"
func NewJSStreamInvalidExternalDeliverySubjError(prefix interface{}, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamInvalidExternalDeliverySubjErrF]
	args := e.toReplacerArgs([]interface{}{"{prefix}", prefix})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamLimitsError creates a new JSStreamLimitsErrF error: "{err}"
func NewJSStreamLimitsError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamLimitsErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamMaxBytesRequiredError creates a new JSStreamMaxBytesRequired error: "account requires a stream config to have max bytes set"
func NewJSStreamMaxBytesRequiredError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamMaxBytesRequired]
}

// NewJSStreamMaxStreamBytesExceededError creates a new JSStreamMaxStreamBytesExceeded error: "stream max bytes exceeds account limit max stream bytes"
func NewJSStreamMaxStreamBytesExceededError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamMaxStreamBytesExceeded]
}

// NewJSStreamMessageExceedsMaximumError creates a new JSStreamMessageExceedsMaximumErr error: "message size exceeds maximum allowed"
func NewJSStreamMessageExceedsMaximumError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamMessageExceedsMaximumErr]
}

// NewJSStreamMinLastSeqError creates a new JSStreamMinLastSeqErr error: "min last sequence"
func NewJSStreamMinLastSeqError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamMinLastSeqErr]
}

// NewJSStreamMirrorNotUpdatableError creates a new JSStreamMirrorNotUpdatableErr error: "stream mirror configuration can not be updated"
func NewJSStreamMirrorNotUpdatableError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamMirrorNotUpdatableErr]
}

// NewJSStreamMismatchError creates a new JSStreamMismatchErr error: "stream name in subject does not match request"
func NewJSStreamMismatchError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamMismatchErr]
}

// NewJSStreamMoveAndScaleError creates a new JSStreamMoveAndScaleErr error: "can not move and scale a stream in a single update"
func NewJSStreamMoveAndScaleError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamMoveAndScaleErr]
}

// NewJSStreamMoveInProgressError creates a new JSStreamMoveInProgressF error: "stream move already in progress: {msg}"
func NewJSStreamMoveInProgressError(msg interface{}, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamMoveInProgressF]
	args := e.toReplacerArgs([]interface{}{"{msg}", msg})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamMoveNotInProgressError creates a new JSStreamMoveNotInProgress error: "stream move not in progress"
func NewJSStreamMoveNotInProgressError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamMoveNotInProgress]
}

// NewJSStreamMsgDeleteFailedError creates a new JSStreamMsgDeleteFailedF error: "{err}"
func NewJSStreamMsgDeleteFailedError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamMsgDeleteFailedF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamNameContainsPathSeparatorsError creates a new JSStreamNameContainsPathSeparatorsErr error: "Stream name can not contain path separators"
func NewJSStreamNameContainsPathSeparatorsError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamNameContainsPathSeparatorsErr]
}

// NewJSStreamNameExistError creates a new JSStreamNameExistErr error: "stream name already in use with a different configuration"
func NewJSStreamNameExistError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamNameExistErr]
}

// NewJSStreamNameExistRestoreFailedError creates a new JSStreamNameExistRestoreFailedErr error: "stream name already in use, cannot restore"
func NewJSStreamNameExistRestoreFailedError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamNameExistRestoreFailedErr]
}

// NewJSStreamNotFoundError creates a new JSStreamNotFoundErr error: "stream not found"
func NewJSStreamNotFoundError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamNotFoundErr]
}

// NewJSStreamNotMatchError creates a new JSStreamNotMatchErr error: "expected stream does not match"
func NewJSStreamNotMatchError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamNotMatchErr]
}

// NewJSStreamOfflineError creates a new JSStreamOfflineErr error: "stream is offline"
func NewJSStreamOfflineError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamOfflineErr]
}

// NewJSStreamOfflineReasonError creates a new JSStreamOfflineReasonErrF error: "stream is offline: {err}"
func NewJSStreamOfflineReasonError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamOfflineReasonErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamPurgeFailedError creates a new JSStreamPurgeFailedF error: "{err}"
func NewJSStreamPurgeFailedError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamPurgeFailedF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamReplicasNotSupportedError creates a new JSStreamReplicasNotSupportedErr error: "replicas > 1 not supported in non-clustered mode"
func NewJSStreamReplicasNotSupportedError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamReplicasNotSupportedErr]
}

// NewJSStreamReplicasNotUpdatableError creates a new JSStreamReplicasNotUpdatableErr error: "Replicas configuration can not be updated"
func NewJSStreamReplicasNotUpdatableError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamReplicasNotUpdatableErr]
}

// NewJSStreamRestoreError creates a new JSStreamRestoreErrF error: "restore failed: {err}"
func NewJSStreamRestoreError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamRestoreErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamRollupFailedError creates a new JSStreamRollupFailedF error: "{err}"
func NewJSStreamRollupFailedError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamRollupFailedF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamSealedError creates a new JSStreamSealedErr error: "invalid operation on sealed stream"
func NewJSStreamSealedError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamSealedErr]
}

// NewJSStreamSequenceNotMatchError creates a new JSStreamSequenceNotMatchErr error: "expected stream sequence does not match"
func NewJSStreamSequenceNotMatchError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamSequenceNotMatchErr]
}

// NewJSStreamSnapshotError creates a new JSStreamSnapshotErrF error: "snapshot failed: {err}"
func NewJSStreamSnapshotError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamSnapshotErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamStoreFailedError creates a new JSStreamStoreFailedF error: "{err}"
func NewJSStreamStoreFailedError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamStoreFailedF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamSubjectOverlapError creates a new JSStreamSubjectOverlapErr error: "subjects overlap with an existing stream"
func NewJSStreamSubjectOverlapError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamSubjectOverlapErr]
}

// NewJSStreamTemplateCreateError creates a new JSStreamTemplateCreateErrF error: "{err}"
func NewJSStreamTemplateCreateError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamTemplateCreateErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamTemplateDeleteError creates a new JSStreamTemplateDeleteErrF error: "{err}"
func NewJSStreamTemplateDeleteError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamTemplateDeleteErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamTemplateNotFoundError creates a new JSStreamTemplateNotFoundErr error: "template not found"
func NewJSStreamTemplateNotFoundError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamTemplateNotFoundErr]
}

// NewJSStreamTooManyRequestsError creates a new JSStreamTooManyRequests error: "too many requests"
func NewJSStreamTooManyRequestsError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamTooManyRequests]
}

// NewJSStreamTransformInvalidDestinationError creates a new JSStreamTransformInvalidDestination error: "stream transform: {err}"
func NewJSStreamTransformInvalidDestinationError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamTransformInvalidDestination]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamTransformInvalidSourceError creates a new JSStreamTransformInvalidSource error: "stream transform source: {err}"
func NewJSStreamTransformInvalidSourceError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamTransformInvalidSource]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamUpdateError creates a new JSStreamUpdateErrF error: "{err}"
func NewJSStreamUpdateError(err error, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamUpdateErrF]
	args := e.toReplacerArgs([]interface{}{"{err}", err})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamWrongLastMsgIDError creates a new JSStreamWrongLastMsgIDErrF error: "wrong last msg ID: {id}"
func NewJSStreamWrongLastMsgIDError(id interface{}, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamWrongLastMsgIDErrF]
	args := e.toReplacerArgs([]interface{}{"{id}", id})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSStreamWrongLastSequenceConstantError creates a new JSStreamWrongLastSequenceConstantErr error: "wrong last sequence"
func NewJSStreamWrongLastSequenceConstantError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSStreamWrongLastSequenceConstantErr]
}

// NewJSStreamWrongLastSequenceError creates a new JSStreamWrongLastSequenceErrF error: "wrong last sequence: {seq}"
func NewJSStreamWrongLastSequenceError(seq uint64, opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	e := ApiErrors[JSStreamWrongLastSequenceErrF]
	args := e.toReplacerArgs([]interface{}{"{seq}", seq})
	return &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: strings.NewReplacer(args...).Replace(e.Description),
	}
}

// NewJSTempStorageFailedError creates a new JSTempStorageFailedErr error: "JetStream unable to open temp storage for restore"
func NewJSTempStorageFailedError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSTempStorageFailedErr]
}

// NewJSTemplateNameNotMatchSubjectError creates a new JSTemplateNameNotMatchSubjectErr error: "template name in subject does not match request"
func NewJSTemplateNameNotMatchSubjectError(opts ...ErrorOption) *ApiError {
	eopts := parseOpts(opts)
	if ae, ok := eopts.err.(*ApiError); ok {
		return ae
	}

	return ApiErrors[JSTemplateNameNotMatchSubjectErr]
}
