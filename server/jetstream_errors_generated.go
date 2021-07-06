// Generated code, do not edit. See errors.json and run go generate to update

package server

const (
	// JSAccountResourcesExceededErr resource limits exceeded for account
	JSAccountResourcesExceededErr ErrorIdentifier = 10002

	// JSBadRequestErr bad request
	JSBadRequestErr ErrorIdentifier = 10003

	// JSClusterIncompleteErr incomplete results
	JSClusterIncompleteErr ErrorIdentifier = 10004

	// JSClusterNoPeersErr no suitable peers for placement
	JSClusterNoPeersErr ErrorIdentifier = 10005

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

	// JSConsumerBadDurableNameErr durable name can not contain '.', '*', '>'
	JSConsumerBadDurableNameErr ErrorIdentifier = 10103

	// JSConsumerConfigRequiredErr consumer config required
	JSConsumerConfigRequiredErr ErrorIdentifier = 10078

	// JSConsumerCreateErrF General consumer creation failure string ({err})
	JSConsumerCreateErrF ErrorIdentifier = 10012

	// JSConsumerDeliverCycleErr consumer deliver subject forms a cycle
	JSConsumerDeliverCycleErr ErrorIdentifier = 10081

	// JSConsumerDeliverToWildcardsErr consumer deliver subject has wildcards
	JSConsumerDeliverToWildcardsErr ErrorIdentifier = 10079

	// JSConsumerDirectRequiresEphemeralErr consumer direct requires an ephemeral consumer
	JSConsumerDirectRequiresEphemeralErr ErrorIdentifier = 10091

	// JSConsumerDirectRequiresPushErr consumer direct requires a push based consumer
	JSConsumerDirectRequiresPushErr ErrorIdentifier = 10090

	// JSConsumerDurableNameNotInSubjectErr consumer expected to be durable but no durable name set in subject
	JSConsumerDurableNameNotInSubjectErr ErrorIdentifier = 10016

	// JSConsumerDurableNameNotMatchSubjectErr consumer name in subject does not match durable name in request
	JSConsumerDurableNameNotMatchSubjectErr ErrorIdentifier = 10017

	// JSConsumerDurableNameNotSetErr consumer expected to be durable but a durable name was not set
	JSConsumerDurableNameNotSetErr ErrorIdentifier = 10018

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

	// JSConsumerInvalidPolicyErrF Generic delivery policy error ({err})
	JSConsumerInvalidPolicyErrF ErrorIdentifier = 10094

	// JSConsumerInvalidSamplingErrF failed to parse consumer sampling configuration: {err}
	JSConsumerInvalidSamplingErrF ErrorIdentifier = 10095

	// JSConsumerMaxPendingAckPolicyRequiredErr consumer requires ack policy for max ack pending
	JSConsumerMaxPendingAckPolicyRequiredErr ErrorIdentifier = 10082

	// JSConsumerMaxWaitingNegativeErr consumer max waiting needs to be positive
	JSConsumerMaxWaitingNegativeErr ErrorIdentifier = 10087

	// JSConsumerNameExistErr consumer name already in use
	JSConsumerNameExistErr ErrorIdentifier = 10013

	// JSConsumerNameTooLongErrF consumer name is too long, maximum allowed is {max}
	JSConsumerNameTooLongErrF ErrorIdentifier = 10102

	// JSConsumerNotFoundErr consumer not found
	JSConsumerNotFoundErr ErrorIdentifier = 10014

	// JSConsumerOnMappedErr consumer direct on a mapped consumer
	JSConsumerOnMappedErr ErrorIdentifier = 10092

	// JSConsumerPullNotDurableErr consumer in pull mode requires a durable name
	JSConsumerPullNotDurableErr ErrorIdentifier = 10085

	// JSConsumerPullRequiresAckErr consumer in pull mode requires explicit ack policy
	JSConsumerPullRequiresAckErr ErrorIdentifier = 10084

	// JSConsumerPullWithRateLimitErr consumer in pull mode can not have rate limit set
	JSConsumerPullWithRateLimitErr ErrorIdentifier = 10086

	// JSConsumerPushMaxWaitingErr consumer in push mode can not set max waiting
	JSConsumerPushMaxWaitingErr ErrorIdentifier = 10080

	// JSConsumerReplacementWithDifferentNameErr consumer replacement durable config not the same
	JSConsumerReplacementWithDifferentNameErr ErrorIdentifier = 10106

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

	// JSInsufficientResourcesErr insufficient resources
	JSInsufficientResourcesErr ErrorIdentifier = 10023

	// JSInvalidJSONErr invalid JSON
	JSInvalidJSONErr ErrorIdentifier = 10025

	// JSMaximumConsumersLimitErr maximum consumers limit reached
	JSMaximumConsumersLimitErr ErrorIdentifier = 10026

	// JSMaximumStreamsLimitErr maximum number of streams reached
	JSMaximumStreamsLimitErr ErrorIdentifier = 10027

	// JSMemoryResourcesExceededErr insufficient memory resources available
	JSMemoryResourcesExceededErr ErrorIdentifier = 10028

	// JSMirrorConsumerSetupFailedErrF Generic mirror consumer setup failure string ({err})
	JSMirrorConsumerSetupFailedErrF ErrorIdentifier = 10029

	// JSMirrorMaxMessageSizeTooBigErr stream mirror must have max message size >= source
	JSMirrorMaxMessageSizeTooBigErr ErrorIdentifier = 10030

	// JSMirrorWithSourcesErr stream mirrors can not also contain other sources
	JSMirrorWithSourcesErr ErrorIdentifier = 10031

	// JSMirrorWithStartSeqAndTimeErr stream mirrors can not have both start seq and start time configured
	JSMirrorWithStartSeqAndTimeErr ErrorIdentifier = 10032

	// JSMirrorWithSubjectFiltersErr stream mirrors can not contain filtered subjects
	JSMirrorWithSubjectFiltersErr ErrorIdentifier = 10033

	// JSMirrorWithSubjectsErr stream mirrors can not also contain subjects
	JSMirrorWithSubjectsErr ErrorIdentifier = 10034

	// JSNoAccountErr account not found
	JSNoAccountErr ErrorIdentifier = 10035

	// JSNoMessageFoundErr no message found
	JSNoMessageFoundErr ErrorIdentifier = 10037

	// JSNotEmptyRequestErr expected an empty request payload
	JSNotEmptyRequestErr ErrorIdentifier = 10038

	// JSNotEnabledErr JetStream not enabled
	JSNotEnabledErr ErrorIdentifier = 10076

	// JSNotEnabledForAccountErr JetStream not enabled for account
	JSNotEnabledForAccountErr ErrorIdentifier = 10039

	// JSPeerRemapErr peer remap failed
	JSPeerRemapErr ErrorIdentifier = 10075

	// JSRaftGeneralErrF General RAFT error string ({err})
	JSRaftGeneralErrF ErrorIdentifier = 10041

	// JSRestoreSubscribeFailedErrF JetStream unable to subscribe to restore snapshot {subject}: {err}
	JSRestoreSubscribeFailedErrF ErrorIdentifier = 10042

	// JSSequenceNotFoundErrF sequence {seq} not found
	JSSequenceNotFoundErrF ErrorIdentifier = 10043

	// JSSnapshotDeliverSubjectInvalidErr deliver subject not valid
	JSSnapshotDeliverSubjectInvalidErr ErrorIdentifier = 10015

	// JSSourceConsumerSetupFailedErrF General source consumer setup failure string ({err})
	JSSourceConsumerSetupFailedErrF ErrorIdentifier = 10045

	// JSSourceMaxMessageSizeTooBigErr stream source must have max message size >= target
	JSSourceMaxMessageSizeTooBigErr ErrorIdentifier = 10046

	// JSStorageResourcesExceededErr insufficient storage resources available
	JSStorageResourcesExceededErr ErrorIdentifier = 10047

	// JSStreamAssignmentErrF Generic stream assignment error string ({err})
	JSStreamAssignmentErrF ErrorIdentifier = 10048

	// JSStreamCreateErrF Generic stream creation error string ({err})
	JSStreamCreateErrF ErrorIdentifier = 10049

	// JSStreamDeleteErrF General stream deletion error string ({err})
	JSStreamDeleteErrF ErrorIdentifier = 10050

	// JSStreamExternalApiOverlapErrF stream external api prefix {prefix} must not overlap with {subject}
	JSStreamExternalApiOverlapErrF ErrorIdentifier = 10021

	// JSStreamExternalDelPrefixOverlapsErrF stream external delivery prefix {prefix} overlaps with stream subject {subject}
	JSStreamExternalDelPrefixOverlapsErrF ErrorIdentifier = 10022

	// JSStreamGeneralErrorF General stream failure string ({err})
	JSStreamGeneralErrorF ErrorIdentifier = 10051

	// JSStreamInvalidConfigF Stream configuration validation error string ({err})
	JSStreamInvalidConfigF ErrorIdentifier = 10052

	// JSStreamInvalidErr stream not valid
	JSStreamInvalidErr ErrorIdentifier = 10096

	// JSStreamInvalidExternalDeliverySubjErrF stream external delivery prefix {prefix} must not contain wildcards
	JSStreamInvalidExternalDeliverySubjErrF ErrorIdentifier = 10024

	// JSStreamLimitsErrF General stream limits exceeded error string ({err})
	JSStreamLimitsErrF ErrorIdentifier = 10053

	// JSStreamMaximumConsumersReachedErr maximum consumers limit reached
	JSStreamMaximumConsumersReachedErr ErrorIdentifier = 10097

	// JSStreamMessageExceedsMaximumErr message size exceeds maximum allowed
	JSStreamMessageExceedsMaximumErr ErrorIdentifier = 10054

	// JSStreamMirrorNotUpdatableErr Mirror configuration can not be updated
	JSStreamMirrorNotUpdatableErr ErrorIdentifier = 10055

	// JSStreamMismatchErr stream name in subject does not match request
	JSStreamMismatchErr ErrorIdentifier = 10056

	// JSStreamMsgDeleteFailedF Generic message deletion failure error string ({err})
	JSStreamMsgDeleteFailedF ErrorIdentifier = 10057

	// JSStreamNameExistErr stream name already in use
	JSStreamNameExistErr ErrorIdentifier = 10058

	// JSStreamNotFoundErr stream not found
	JSStreamNotFoundErr ErrorIdentifier = 10059

	// JSStreamNotMatchErr expected stream does not match
	JSStreamNotMatchErr ErrorIdentifier = 10060

	// JSStreamReplicasNotSupportedErr replicas > 1 not supported in non-clustered mode
	JSStreamReplicasNotSupportedErr ErrorIdentifier = 10074

	// JSStreamReplicasNotUpdatableErr Replicas configuration can not be updated
	JSStreamReplicasNotUpdatableErr ErrorIdentifier = 10061

	// JSStreamRestoreErrF restore failed: {err}
	JSStreamRestoreErrF ErrorIdentifier = 10062

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

	// JSStreamUpdateErrF Generic stream update error string ({err})
	JSStreamUpdateErrF ErrorIdentifier = 10069

	// JSStreamWrongLastMsgIDErrF wrong last msg ID: {id}
	JSStreamWrongLastMsgIDErrF ErrorIdentifier = 10070

	// JSStreamWrongLastSequenceErrF wrong last sequence: {seq}
	JSStreamWrongLastSequenceErrF ErrorIdentifier = 10071

	// JSTempStorageFailedErr JetStream unable to open temp storage for restore
	JSTempStorageFailedErr ErrorIdentifier = 10072

	// JSTemplateNameNotMatchSubjectErr template name in subject does not match request
	JSTemplateNameNotMatchSubjectErr ErrorIdentifier = 10073
)

var (
	ApiErrors = map[ErrorIdentifier]*ApiError{
		JSAccountResourcesExceededErr:              {Code: 400, ErrCode: 10002, Description: "resource limits exceeded for account"},
		JSBadRequestErr:                            {Code: 400, ErrCode: 10003, Description: "bad request"},
		JSClusterIncompleteErr:                     {Code: 503, ErrCode: 10004, Description: "incomplete results"},
		JSClusterNoPeersErr:                        {Code: 400, ErrCode: 10005, Description: "no suitable peers for placement"},
		JSClusterNotActiveErr:                      {Code: 500, ErrCode: 10006, Description: "JetStream not in clustered mode"},
		JSClusterNotAssignedErr:                    {Code: 500, ErrCode: 10007, Description: "JetStream cluster not assigned to this server"},
		JSClusterNotAvailErr:                       {Code: 503, ErrCode: 10008, Description: "JetStream system temporarily unavailable"},
		JSClusterNotLeaderErr:                      {Code: 500, ErrCode: 10009, Description: "JetStream cluster can not handle request"},
		JSClusterPeerNotMemberErr:                  {Code: 400, ErrCode: 10040, Description: "peer not a member"},
		JSClusterRequiredErr:                       {Code: 503, ErrCode: 10010, Description: "JetStream clustering support required"},
		JSClusterServerNotMemberErr:                {Code: 400, ErrCode: 10044, Description: "server is not a member of the cluster"},
		JSClusterTagsErr:                           {Code: 400, ErrCode: 10011, Description: "tags placement not supported for operation"},
		JSClusterUnSupportFeatureErr:               {Code: 503, ErrCode: 10036, Description: "not currently supported in clustered mode"},
		JSConsumerBadDurableNameErr:                {Code: 400, ErrCode: 10103, Description: "durable name can not contain '.', '*', '>'"},
		JSConsumerConfigRequiredErr:                {Code: 400, ErrCode: 10078, Description: "consumer config required"},
		JSConsumerCreateErrF:                       {Code: 500, ErrCode: 10012, Description: "{err}"},
		JSConsumerDeliverCycleErr:                  {Code: 400, ErrCode: 10081, Description: "consumer deliver subject forms a cycle"},
		JSConsumerDeliverToWildcardsErr:            {Code: 400, ErrCode: 10079, Description: "consumer deliver subject has wildcards"},
		JSConsumerDirectRequiresEphemeralErr:       {Code: 400, ErrCode: 10091, Description: "consumer direct requires an ephemeral consumer"},
		JSConsumerDirectRequiresPushErr:            {Code: 400, ErrCode: 10090, Description: "consumer direct requires a push based consumer"},
		JSConsumerDurableNameNotInSubjectErr:       {Code: 400, ErrCode: 10016, Description: "consumer expected to be durable but no durable name set in subject"},
		JSConsumerDurableNameNotMatchSubjectErr:    {Code: 400, ErrCode: 10017, Description: "consumer name in subject does not match durable name in request"},
		JSConsumerDurableNameNotSetErr:             {Code: 400, ErrCode: 10018, Description: "consumer expected to be durable but a durable name was not set"},
		JSConsumerEphemeralWithDurableInSubjectErr: {Code: 400, ErrCode: 10019, Description: "consumer expected to be ephemeral but detected a durable name set in subject"},
		JSConsumerEphemeralWithDurableNameErr:      {Code: 400, ErrCode: 10020, Description: "consumer expected to be ephemeral but a durable name was set in request"},
		JSConsumerExistingActiveErr:                {Code: 400, ErrCode: 10105, Description: "consumer already exists and is still active"},
		JSConsumerFCRequiresPushErr:                {Code: 400, ErrCode: 10089, Description: "consumer flow control requires a push based consumer"},
		JSConsumerFilterNotSubsetErr:               {Code: 400, ErrCode: 10093, Description: "consumer filter subject is not a valid subset of the interest subjects"},
		JSConsumerHBRequiresPushErr:                {Code: 400, ErrCode: 10088, Description: "consumer idle heartbeat requires a push based consumer"},
		JSConsumerInvalidPolicyErrF:                {Code: 400, ErrCode: 10094, Description: "{err}"},
		JSConsumerInvalidSamplingErrF:              {Code: 400, ErrCode: 10095, Description: "failed to parse consumer sampling configuration: {err}"},
		JSConsumerMaxPendingAckPolicyRequiredErr:   {Code: 400, ErrCode: 10082, Description: "consumer requires ack policy for max ack pending"},
		JSConsumerMaxWaitingNegativeErr:            {Code: 400, ErrCode: 10087, Description: "consumer max waiting needs to be positive"},
		JSConsumerNameExistErr:                     {Code: 400, ErrCode: 10013, Description: "consumer name already in use"},
		JSConsumerNameTooLongErrF:                  {Code: 400, ErrCode: 10102, Description: "consumer name is too long, maximum allowed is {max}"},
		JSConsumerNotFoundErr:                      {Code: 404, ErrCode: 10014, Description: "consumer not found"},
		JSConsumerOnMappedErr:                      {Code: 400, ErrCode: 10092, Description: "consumer direct on a mapped consumer"},
		JSConsumerPullNotDurableErr:                {Code: 400, ErrCode: 10085, Description: "consumer in pull mode requires a durable name"},
		JSConsumerPullRequiresAckErr:               {Code: 400, ErrCode: 10084, Description: "consumer in pull mode requires explicit ack policy"},
		JSConsumerPullWithRateLimitErr:             {Code: 400, ErrCode: 10086, Description: "consumer in pull mode can not have rate limit set"},
		JSConsumerPushMaxWaitingErr:                {Code: 400, ErrCode: 10080, Description: "consumer in push mode can not set max waiting"},
		JSConsumerReplacementWithDifferentNameErr:  {Code: 400, ErrCode: 10106, Description: "consumer replacement durable config not the same"},
		JSConsumerSmallHeartbeatErr:                {Code: 400, ErrCode: 10083, Description: "consumer idle heartbeat needs to be >= 100ms"},
		JSConsumerStoreFailedErrF:                  {Code: 500, ErrCode: 10104, Description: "error creating store for consumer: {err}"},
		JSConsumerWQConsumerNotDeliverAllErr:       {Code: 400, ErrCode: 10101, Description: "consumer must be deliver all on workqueue stream"},
		JSConsumerWQConsumerNotUniqueErr:           {Code: 400, ErrCode: 10100, Description: "filtered consumer not unique on workqueue stream"},
		JSConsumerWQMultipleUnfilteredErr:          {Code: 400, ErrCode: 10099, Description: "multiple non-filtered consumers not allowed on workqueue stream"},
		JSConsumerWQRequiresExplicitAckErr:         {Code: 400, ErrCode: 10098, Description: "workqueue stream requires explicit ack"},
		JSInsufficientResourcesErr:                 {Code: 503, ErrCode: 10023, Description: "insufficient resources"},
		JSInvalidJSONErr:                           {Code: 400, ErrCode: 10025, Description: "invalid JSON"},
		JSMaximumConsumersLimitErr:                 {Code: 400, ErrCode: 10026, Description: "maximum consumers limit reached"},
		JSMaximumStreamsLimitErr:                   {Code: 400, ErrCode: 10027, Description: "maximum number of streams reached"},
		JSMemoryResourcesExceededErr:               {Code: 500, ErrCode: 10028, Description: "insufficient memory resources available"},
		JSMirrorConsumerSetupFailedErrF:            {Code: 500, ErrCode: 10029, Description: "{err}"},
		JSMirrorMaxMessageSizeTooBigErr:            {Code: 400, ErrCode: 10030, Description: "stream mirror must have max message size >= source"},
		JSMirrorWithSourcesErr:                     {Code: 400, ErrCode: 10031, Description: "stream mirrors can not also contain other sources"},
		JSMirrorWithStartSeqAndTimeErr:             {Code: 400, ErrCode: 10032, Description: "stream mirrors can not have both start seq and start time configured"},
		JSMirrorWithSubjectFiltersErr:              {Code: 400, ErrCode: 10033, Description: "stream mirrors can not contain filtered subjects"},
		JSMirrorWithSubjectsErr:                    {Code: 400, ErrCode: 10034, Description: "stream mirrors can not also contain subjects"},
		JSNoAccountErr:                             {Code: 503, ErrCode: 10035, Description: "account not found"},
		JSNoMessageFoundErr:                        {Code: 404, ErrCode: 10037, Description: "no message found"},
		JSNotEmptyRequestErr:                       {Code: 400, ErrCode: 10038, Description: "expected an empty request payload"},
		JSNotEnabledErr:                            {Code: 503, ErrCode: 10076, Description: "JetStream not enabled"},
		JSNotEnabledForAccountErr:                  {Code: 503, ErrCode: 10039, Description: "JetStream not enabled for account"},
		JSPeerRemapErr:                             {Code: 503, ErrCode: 10075, Description: "peer remap failed"},
		JSRaftGeneralErrF:                          {Code: 500, ErrCode: 10041, Description: "{err}"},
		JSRestoreSubscribeFailedErrF:               {Code: 500, ErrCode: 10042, Description: "JetStream unable to subscribe to restore snapshot {subject}: {err}"},
		JSSequenceNotFoundErrF:                     {Code: 400, ErrCode: 10043, Description: "sequence {seq} not found"},
		JSSnapshotDeliverSubjectInvalidErr:         {Code: 400, ErrCode: 10015, Description: "deliver subject not valid"},
		JSSourceConsumerSetupFailedErrF:            {Code: 500, ErrCode: 10045, Description: "{err}"},
		JSSourceMaxMessageSizeTooBigErr:            {Code: 400, ErrCode: 10046, Description: "stream source must have max message size >= target"},
		JSStorageResourcesExceededErr:              {Code: 500, ErrCode: 10047, Description: "insufficient storage resources available"},
		JSStreamAssignmentErrF:                     {Code: 500, ErrCode: 10048, Description: "{err}"},
		JSStreamCreateErrF:                         {Code: 500, ErrCode: 10049, Description: "{err}"},
		JSStreamDeleteErrF:                         {Code: 500, ErrCode: 10050, Description: "{err}"},
		JSStreamExternalApiOverlapErrF:             {Code: 400, ErrCode: 10021, Description: "stream external api prefix {prefix} must not overlap with {subject}"},
		JSStreamExternalDelPrefixOverlapsErrF:      {Code: 400, ErrCode: 10022, Description: "stream external delivery prefix {prefix} overlaps with stream subject {subject}"},
		JSStreamGeneralErrorF:                      {Code: 500, ErrCode: 10051, Description: "{err}"},
		JSStreamInvalidConfigF:                     {Code: 500, ErrCode: 10052, Description: "{err}"},
		JSStreamInvalidErr:                         {Code: 500, ErrCode: 10096, Description: "stream not valid"},
		JSStreamInvalidExternalDeliverySubjErrF:    {Code: 400, ErrCode: 10024, Description: "stream external delivery prefix {prefix} must not contain wildcards"},
		JSStreamLimitsErrF:                         {Code: 500, ErrCode: 10053, Description: "{err}"},
		JSStreamMaximumConsumersReachedErr:         {Code: 400, ErrCode: 10097, Description: "maximum consumers limit reached"},
		JSStreamMessageExceedsMaximumErr:           {Code: 400, ErrCode: 10054, Description: "message size exceeds maximum allowed"},
		JSStreamMirrorNotUpdatableErr:              {Code: 400, ErrCode: 10055, Description: "Mirror configuration can not be updated"},
		JSStreamMismatchErr:                        {Code: 400, ErrCode: 10056, Description: "stream name in subject does not match request"},
		JSStreamMsgDeleteFailedF:                   {Code: 500, ErrCode: 10057, Description: "{err}"},
		JSStreamNameExistErr:                       {Code: 400, ErrCode: 10058, Description: "stream name already in use"},
		JSStreamNotFoundErr:                        {Code: 404, ErrCode: 10059, Description: "stream not found"},
		JSStreamNotMatchErr:                        {Code: 400, ErrCode: 10060, Description: "expected stream does not match"},
		JSStreamReplicasNotSupportedErr:            {Code: 500, ErrCode: 10074, Description: "replicas > 1 not supported in non-clustered mode"},
		JSStreamReplicasNotUpdatableErr:            {Code: 400, ErrCode: 10061, Description: "Replicas configuration can not be updated"},
		JSStreamRestoreErrF:                        {Code: 500, ErrCode: 10062, Description: "restore failed: {err}"},
		JSStreamSequenceNotMatchErr:                {Code: 503, ErrCode: 10063, Description: "expected stream sequence does not match"},
		JSStreamSnapshotErrF:                       {Code: 500, ErrCode: 10064, Description: "snapshot failed: {err}"},
		JSStreamStoreFailedF:                       {Code: 503, ErrCode: 10077, Description: "{err}"},
		JSStreamSubjectOverlapErr:                  {Code: 500, ErrCode: 10065, Description: "subjects overlap with an existing stream"},
		JSStreamTemplateCreateErrF:                 {Code: 500, ErrCode: 10066, Description: "{err}"},
		JSStreamTemplateDeleteErrF:                 {Code: 500, ErrCode: 10067, Description: "{err}"},
		JSStreamTemplateNotFoundErr:                {Code: 404, ErrCode: 10068, Description: "template not found"},
		JSStreamUpdateErrF:                         {Code: 500, ErrCode: 10069, Description: "{err}"},
		JSStreamWrongLastMsgIDErrF:                 {Code: 400, ErrCode: 10070, Description: "wrong last msg ID: {id}"},
		JSStreamWrongLastSequenceErrF:              {Code: 400, ErrCode: 10071, Description: "wrong last sequence: {seq}"},
		JSTempStorageFailedErr:                     {Code: 500, ErrCode: 10072, Description: "JetStream unable to open temp storage for restore"},
		JSTemplateNameNotMatchSubjectErr:           {Code: 400, ErrCode: 10073, Description: "template name in subject does not match request"},
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
