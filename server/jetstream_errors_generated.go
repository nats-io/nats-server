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

	// JSConsumerCreateErrF General consumer creation failure string ({err})
	JSConsumerCreateErrF ErrorIdentifier = 10012

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

	// JSConsumerNameExistErr consumer name already in use
	JSConsumerNameExistErr ErrorIdentifier = 10013

	// JSConsumerNotFoundErr consumer not found
	JSConsumerNotFoundErr ErrorIdentifier = 10014

	// JSInsufficientResourcesErr insufficient resources
	JSInsufficientResourcesErr ErrorIdentifier = 10023

	// JSInvalidJSONErr invalid JSON
	JSInvalidJSONErr ErrorIdentifier = 10025

	// JSMaximumConsumersLimitErr maximum consumers exceeds account limit
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

	// JSStreamGeneralErrorF General stream failure string
	JSStreamGeneralErrorF ErrorIdentifier = 10051

	// JSStreamInvalidConfigF Stream configuration validation error string ({err})
	JSStreamInvalidConfigF ErrorIdentifier = 10052

	// JSStreamInvalidExternalDeliverySubjErrF stream external delivery prefix {prefix} must not contain wildcards
	JSStreamInvalidExternalDeliverySubjErrF ErrorIdentifier = 10024

	// JSStreamLimitsErrF General stream limits exceeded error string ({err})
	JSStreamLimitsErrF ErrorIdentifier = 10053

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
		JSConsumerCreateErrF:                       {Code: 500, ErrCode: 10012, Description: "{err}"},
		JSConsumerDurableNameNotInSubjectErr:       {Code: 400, ErrCode: 10016, Description: "consumer expected to be durable but no durable name set in subject"},
		JSConsumerDurableNameNotMatchSubjectErr:    {Code: 400, ErrCode: 10017, Description: "consumer name in subject does not match durable name in request"},
		JSConsumerDurableNameNotSetErr:             {Code: 400, ErrCode: 10018, Description: "consumer expected to be durable but a durable name was not set"},
		JSConsumerEphemeralWithDurableInSubjectErr: {Code: 400, ErrCode: 10019, Description: "consumer expected to be ephemeral but detected a durable name set in subject"},
		JSConsumerEphemeralWithDurableNameErr:      {Code: 400, ErrCode: 10020, Description: "consumer expected to be ephemeral but a durable name was set in request"},
		JSConsumerNameExistErr:                     {Code: 400, ErrCode: 10013, Description: "consumer name already in use"},
		JSConsumerNotFoundErr:                      {Code: 404, ErrCode: 10014, Description: "consumer not found"},
		JSInsufficientResourcesErr:                 {Code: 503, ErrCode: 10023, Description: "insufficient resources"},
		JSInvalidJSONErr:                           {Code: 400, ErrCode: 10025, Description: "invalid JSON"},
		JSMaximumConsumersLimitErr:                 {Code: 400, ErrCode: 10026, Description: "maximum consumers exceeds account limit"},
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
		JSNotEnabledErr:                            {Code: 503, ErrCode: 10076, Description: "JetStream not enabled", Help: "This error indicates that JetStream is not enabled at a global level"},
		JSNotEnabledForAccountErr:                  {Code: 503, ErrCode: 10039, Description: "JetStream not enabled for account", Help: "This error indicates that JetStream is not enabled for an account account level"},
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
		JSStreamGeneralErrorF:                      {Code: 500, ErrCode: 10051, Description: "General stream failure string"},
		JSStreamInvalidConfigF:                     {Code: 500, ErrCode: 10052, Description: "{err}"},
		JSStreamInvalidExternalDeliverySubjErrF:    {Code: 400, ErrCode: 10024, Description: "stream external delivery prefix {prefix} must not contain wildcards"},
		JSStreamLimitsErrF:                         {Code: 500, ErrCode: 10053, Description: "{err}"},
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

	errorsDataSource = "W3siY29uc3RhbnQiOiJKU0FjY291bnRSZXNvdXJjZXNFeGNlZWRlZEVyciIsImNvZGUiOjQwMCwiZXJyb3JfY29kZSI6MTAwMDIsImRlc2NyaXB0aW9uIjoicmVzb3VyY2UgbGltaXRzIGV4Y2VlZGVkIGZvciBhY2NvdW50IiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTQmFkUmVxdWVzdEVyciIsImNvZGUiOjQwMCwiZXJyb3JfY29kZSI6MTAwMDMsImRlc2NyaXB0aW9uIjoiYmFkIHJlcXVlc3QiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNDbHVzdGVySW5jb21wbGV0ZUVyciIsImNvZGUiOjUwMywiZXJyb3JfY29kZSI6MTAwMDQsImRlc2NyaXB0aW9uIjoiaW5jb21wbGV0ZSByZXN1bHRzIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTQ2x1c3Rlck5vUGVlcnNFcnIiLCJjb2RlIjo0MDAsImVycm9yX2NvZGUiOjEwMDA1LCJkZXNjcmlwdGlvbiI6Im5vIHN1aXRhYmxlIHBlZXJzIGZvciBwbGFjZW1lbnQiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNDbHVzdGVyTm90QWN0aXZlRXJyIiwiY29kZSI6NTAwLCJlcnJvcl9jb2RlIjoxMDAwNiwiZGVzY3JpcHRpb24iOiJKZXRTdHJlYW0gbm90IGluIGNsdXN0ZXJlZCBtb2RlIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiRXJySmV0U3RyZWFtTm90Q2x1c3RlcmVkIn0seyJjb25zdGFudCI6IkpTQ2x1c3Rlck5vdEFzc2lnbmVkRXJyIiwiY29kZSI6NTAwLCJlcnJvcl9jb2RlIjoxMDAwNywiZGVzY3JpcHRpb24iOiJKZXRTdHJlYW0gY2x1c3RlciBub3QgYXNzaWduZWQgdG8gdGhpcyBzZXJ2ZXIiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiJFcnJKZXRTdHJlYW1Ob3RBc3NpZ25lZCJ9LHsiY29uc3RhbnQiOiJKU0NsdXN0ZXJOb3RBdmFpbEVyciIsImNvZGUiOjUwMywiZXJyb3JfY29kZSI6MTAwMDgsImRlc2NyaXB0aW9uIjoiSmV0U3RyZWFtIHN5c3RlbSB0ZW1wb3JhcmlseSB1bmF2YWlsYWJsZSIsImNvbW1lbnQiOiIiLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IiJ9LHsiY29uc3RhbnQiOiJKU0NsdXN0ZXJOb3RMZWFkZXJFcnIiLCJjb2RlIjo1MDAsImVycm9yX2NvZGUiOjEwMDA5LCJkZXNjcmlwdGlvbiI6IkpldFN0cmVhbSBjbHVzdGVyIGNhbiBub3QgaGFuZGxlIHJlcXVlc3QiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiJFcnJKZXRTdHJlYW1Ob3RMZWFkZXIifSx7ImNvbnN0YW50IjoiSlNDbHVzdGVyUGVlck5vdE1lbWJlckVyciIsImNvZGUiOjQwMCwiZXJyb3JfY29kZSI6MTAwNDAsImRlc2NyaXB0aW9uIjoicGVlciBub3QgYSBtZW1iZXIiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNDbHVzdGVyUmVxdWlyZWRFcnIiLCJjb2RlIjo1MDMsImVycm9yX2NvZGUiOjEwMDEwLCJkZXNjcmlwdGlvbiI6IkpldFN0cmVhbSBjbHVzdGVyaW5nIHN1cHBvcnQgcmVxdWlyZWQiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNDbHVzdGVyU2VydmVyTm90TWVtYmVyRXJyIiwiY29kZSI6NDAwLCJlcnJvcl9jb2RlIjoxMDA0NCwiZGVzY3JpcHRpb24iOiJzZXJ2ZXIgaXMgbm90IGEgbWVtYmVyIG9mIHRoZSBjbHVzdGVyIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTQ2x1c3RlclRhZ3NFcnIiLCJjb2RlIjo0MDAsImVycm9yX2NvZGUiOjEwMDExLCJkZXNjcmlwdGlvbiI6InRhZ3MgcGxhY2VtZW50IG5vdCBzdXBwb3J0ZWQgZm9yIG9wZXJhdGlvbiIsImNvbW1lbnQiOiIiLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IiJ9LHsiY29uc3RhbnQiOiJKU0NsdXN0ZXJVblN1cHBvcnRGZWF0dXJlRXJyIiwiY29kZSI6NTAzLCJlcnJvcl9jb2RlIjoxMDAzNiwiZGVzY3JpcHRpb24iOiJub3QgY3VycmVudGx5IHN1cHBvcnRlZCBpbiBjbHVzdGVyZWQgbW9kZSIsImNvbW1lbnQiOiIiLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IiJ9LHsiY29uc3RhbnQiOiJKU0NvbnN1bWVyQ3JlYXRlRXJyRiIsImNvZGUiOjUwMCwiZXJyb3JfY29kZSI6MTAwMTIsImRlc2NyaXB0aW9uIjoie2Vycn0iLCJjb21tZW50IjoiR2VuZXJhbCBjb25zdW1lciBjcmVhdGlvbiBmYWlsdXJlIHN0cmluZyIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTQ29uc3VtZXJEdXJhYmxlTmFtZU5vdEluU3ViamVjdEVyciIsImNvZGUiOjQwMCwiZXJyb3JfY29kZSI6MTAwMTYsImRlc2NyaXB0aW9uIjoiY29uc3VtZXIgZXhwZWN0ZWQgdG8gYmUgZHVyYWJsZSBidXQgbm8gZHVyYWJsZSBuYW1lIHNldCBpbiBzdWJqZWN0IiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTQ29uc3VtZXJEdXJhYmxlTmFtZU5vdE1hdGNoU3ViamVjdEVyciIsImNvZGUiOjQwMCwiZXJyb3JfY29kZSI6MTAwMTcsImRlc2NyaXB0aW9uIjoiY29uc3VtZXIgbmFtZSBpbiBzdWJqZWN0IGRvZXMgbm90IG1hdGNoIGR1cmFibGUgbmFtZSBpbiByZXF1ZXN0IiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTQ29uc3VtZXJEdXJhYmxlTmFtZU5vdFNldEVyciIsImNvZGUiOjQwMCwiZXJyb3JfY29kZSI6MTAwMTgsImRlc2NyaXB0aW9uIjoiY29uc3VtZXIgZXhwZWN0ZWQgdG8gYmUgZHVyYWJsZSBidXQgYSBkdXJhYmxlIG5hbWUgd2FzIG5vdCBzZXQiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNDb25zdW1lckVwaGVtZXJhbFdpdGhEdXJhYmxlSW5TdWJqZWN0RXJyIiwiY29kZSI6NDAwLCJlcnJvcl9jb2RlIjoxMDAxOSwiZGVzY3JpcHRpb24iOiJjb25zdW1lciBleHBlY3RlZCB0byBiZSBlcGhlbWVyYWwgYnV0IGRldGVjdGVkIGEgZHVyYWJsZSBuYW1lIHNldCBpbiBzdWJqZWN0IiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTQ29uc3VtZXJFcGhlbWVyYWxXaXRoRHVyYWJsZU5hbWVFcnIiLCJjb2RlIjo0MDAsImVycm9yX2NvZGUiOjEwMDIwLCJkZXNjcmlwdGlvbiI6ImNvbnN1bWVyIGV4cGVjdGVkIHRvIGJlIGVwaGVtZXJhbCBidXQgYSBkdXJhYmxlIG5hbWUgd2FzIHNldCBpbiByZXF1ZXN0IiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTQ29uc3VtZXJOYW1lRXhpc3RFcnIiLCJjb2RlIjo0MDAsImVycm9yX2NvZGUiOjEwMDEzLCJkZXNjcmlwdGlvbiI6ImNvbnN1bWVyIG5hbWUgYWxyZWFkeSBpbiB1c2UiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiJFcnJKZXRTdHJlYW1Db25zdW1lckFscmVhZHlVc2VkIn0seyJjb25zdGFudCI6IkpTQ29uc3VtZXJOb3RGb3VuZEVyciIsImNvZGUiOjQwNCwiZXJyb3JfY29kZSI6MTAwMTQsImRlc2NyaXB0aW9uIjoiY29uc3VtZXIgbm90IGZvdW5kIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTSW5zdWZmaWNpZW50UmVzb3VyY2VzRXJyIiwiY29kZSI6NTAzLCJlcnJvcl9jb2RlIjoxMDAyMywiZGVzY3JpcHRpb24iOiJpbnN1ZmZpY2llbnQgcmVzb3VyY2VzIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiRXJySmV0U3RyZWFtUmVzb3VyY2VzRXhjZWVkZWQifSx7ImNvbnN0YW50IjoiSlNJbnZhbGlkSlNPTkVyciIsImNvZGUiOjQwMCwiZXJyb3JfY29kZSI6MTAwMjUsImRlc2NyaXB0aW9uIjoiaW52YWxpZCBKU09OIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTTWF4aW11bUNvbnN1bWVyc0xpbWl0RXJyIiwiY29kZSI6NDAwLCJlcnJvcl9jb2RlIjoxMDAyNiwiZGVzY3JpcHRpb24iOiJtYXhpbXVtIGNvbnN1bWVycyBleGNlZWRzIGFjY291bnQgbGltaXQiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNNYXhpbXVtU3RyZWFtc0xpbWl0RXJyIiwiY29kZSI6NDAwLCJlcnJvcl9jb2RlIjoxMDAyNywiZGVzY3JpcHRpb24iOiJtYXhpbXVtIG51bWJlciBvZiBzdHJlYW1zIHJlYWNoZWQiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNNZW1vcnlSZXNvdXJjZXNFeGNlZWRlZEVyciIsImNvZGUiOjUwMCwiZXJyb3JfY29kZSI6MTAwMjgsImRlc2NyaXB0aW9uIjoiaW5zdWZmaWNpZW50IG1lbW9yeSByZXNvdXJjZXMgYXZhaWxhYmxlIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiRXJyTWVtb3J5UmVzb3VyY2VzRXhjZWVkZWQifSx7ImNvbnN0YW50IjoiSlNNaXJyb3JDb25zdW1lclNldHVwRmFpbGVkRXJyRiIsImNvZGUiOjUwMCwiZXJyb3JfY29kZSI6MTAwMjksImRlc2NyaXB0aW9uIjoie2Vycn0iLCJjb21tZW50IjoiR2VuZXJpYyBtaXJyb3IgY29uc3VtZXIgc2V0dXAgZmFpbHVyZSBzdHJpbmciLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IiJ9LHsiY29uc3RhbnQiOiJKU01pcnJvck1heE1lc3NhZ2VTaXplVG9vQmlnRXJyIiwiY29kZSI6NDAwLCJlcnJvcl9jb2RlIjoxMDAzMCwiZGVzY3JpcHRpb24iOiJzdHJlYW0gbWlycm9yIG11c3QgaGF2ZSBtYXggbWVzc2FnZSBzaXplIFx1MDAzZT0gc291cmNlIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTTWlycm9yV2l0aFNvdXJjZXNFcnIiLCJjb2RlIjo0MDAsImVycm9yX2NvZGUiOjEwMDMxLCJkZXNjcmlwdGlvbiI6InN0cmVhbSBtaXJyb3JzIGNhbiBub3QgYWxzbyBjb250YWluIG90aGVyIHNvdXJjZXMiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNNaXJyb3JXaXRoU3RhcnRTZXFBbmRUaW1lRXJyIiwiY29kZSI6NDAwLCJlcnJvcl9jb2RlIjoxMDAzMiwiZGVzY3JpcHRpb24iOiJzdHJlYW0gbWlycm9ycyBjYW4gbm90IGhhdmUgYm90aCBzdGFydCBzZXEgYW5kIHN0YXJ0IHRpbWUgY29uZmlndXJlZCIsImNvbW1lbnQiOiIiLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IiJ9LHsiY29uc3RhbnQiOiJKU01pcnJvcldpdGhTdWJqZWN0RmlsdGVyc0VyciIsImNvZGUiOjQwMCwiZXJyb3JfY29kZSI6MTAwMzMsImRlc2NyaXB0aW9uIjoic3RyZWFtIG1pcnJvcnMgY2FuIG5vdCBjb250YWluIGZpbHRlcmVkIHN1YmplY3RzIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTTWlycm9yV2l0aFN1YmplY3RzRXJyIiwiY29kZSI6NDAwLCJlcnJvcl9jb2RlIjoxMDAzNCwiZGVzY3JpcHRpb24iOiJzdHJlYW0gbWlycm9ycyBjYW4gbm90IGFsc28gY29udGFpbiBzdWJqZWN0cyIsImNvbW1lbnQiOiIiLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IiJ9LHsiY29uc3RhbnQiOiJKU05vQWNjb3VudEVyciIsImNvZGUiOjUwMywiZXJyb3JfY29kZSI6MTAwMzUsImRlc2NyaXB0aW9uIjoiYWNjb3VudCBub3QgZm91bmQiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNOb01lc3NhZ2VGb3VuZEVyciIsImNvZGUiOjQwNCwiZXJyb3JfY29kZSI6MTAwMzcsImRlc2NyaXB0aW9uIjoibm8gbWVzc2FnZSBmb3VuZCIsImNvbW1lbnQiOiIiLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IiJ9LHsiY29uc3RhbnQiOiJKU05vdEVtcHR5UmVxdWVzdEVyciIsImNvZGUiOjQwMCwiZXJyb3JfY29kZSI6MTAwMzgsImRlc2NyaXB0aW9uIjoiZXhwZWN0ZWQgYW4gZW1wdHkgcmVxdWVzdCBwYXlsb2FkIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTTm90RW5hYmxlZEVyciIsImNvZGUiOjUwMywiZXJyb3JfY29kZSI6MTAwNzYsImRlc2NyaXB0aW9uIjoiSmV0U3RyZWFtIG5vdCBlbmFibGVkIiwiY29tbWVudCI6IiIsImhlbHAiOiJUaGlzIGVycm9yIGluZGljYXRlcyB0aGF0IEpldFN0cmVhbSBpcyBub3QgZW5hYmxlZCBhdCBhIGdsb2JhbCBsZXZlbCIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiJFcnJKZXRTdHJlYW1Ob3RFbmFibGVkIn0seyJjb25zdGFudCI6IkpTTm90RW5hYmxlZEZvckFjY291bnRFcnIiLCJjb2RlIjo1MDMsImVycm9yX2NvZGUiOjEwMDM5LCJkZXNjcmlwdGlvbiI6IkpldFN0cmVhbSBub3QgZW5hYmxlZCBmb3IgYWNjb3VudCIsImNvbW1lbnQiOiIiLCJoZWxwIjoiVGhpcyBlcnJvciBpbmRpY2F0ZXMgdGhhdCBKZXRTdHJlYW0gaXMgbm90IGVuYWJsZWQgZm9yIGFuIGFjY291bnQgYWNjb3VudCBsZXZlbCIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNQZWVyUmVtYXBFcnIiLCJjb2RlIjo1MDMsImVycm9yX2NvZGUiOjEwMDc1LCJkZXNjcmlwdGlvbiI6InBlZXIgcmVtYXAgZmFpbGVkIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTUmFmdEdlbmVyYWxFcnJGIiwiY29kZSI6NTAwLCJlcnJvcl9jb2RlIjoxMDA0MSwiZGVzY3JpcHRpb24iOiJ7ZXJyfSIsImNvbW1lbnQiOiJHZW5lcmFsIFJBRlQgZXJyb3Igc3RyaW5nIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNSZXN0b3JlU3Vic2NyaWJlRmFpbGVkRXJyRiIsImNvZGUiOjUwMCwiZXJyb3JfY29kZSI6MTAwNDIsImRlc2NyaXB0aW9uIjoiSmV0U3RyZWFtIHVuYWJsZSB0byBzdWJzY3JpYmUgdG8gcmVzdG9yZSBzbmFwc2hvdCB7c3ViamVjdH06IHtlcnJ9IiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTU2VxdWVuY2VOb3RGb3VuZEVyckYiLCJjb2RlIjo0MDAsImVycm9yX2NvZGUiOjEwMDQzLCJkZXNjcmlwdGlvbiI6InNlcXVlbmNlIHtzZXF9IG5vdCBmb3VuZCIsImNvbW1lbnQiOiIiLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IiJ9LHsiY29uc3RhbnQiOiJKU1NuYXBzaG90RGVsaXZlclN1YmplY3RJbnZhbGlkRXJyIiwiY29kZSI6NDAwLCJlcnJvcl9jb2RlIjoxMDAxNSwiZGVzY3JpcHRpb24iOiJkZWxpdmVyIHN1YmplY3Qgbm90IHZhbGlkIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTU291cmNlQ29uc3VtZXJTZXR1cEZhaWxlZEVyckYiLCJjb2RlIjo1MDAsImVycm9yX2NvZGUiOjEwMDQ1LCJkZXNjcmlwdGlvbiI6IntlcnJ9IiwiY29tbWVudCI6IkdlbmVyYWwgc291cmNlIGNvbnN1bWVyIHNldHVwIGZhaWx1cmUgc3RyaW5nIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNTb3VyY2VNYXhNZXNzYWdlU2l6ZVRvb0JpZ0VyciIsImNvZGUiOjQwMCwiZXJyb3JfY29kZSI6MTAwNDYsImRlc2NyaXB0aW9uIjoic3RyZWFtIHNvdXJjZSBtdXN0IGhhdmUgbWF4IG1lc3NhZ2Ugc2l6ZSBcdTAwM2U9IHRhcmdldCIsImNvbW1lbnQiOiIiLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IiJ9LHsiY29uc3RhbnQiOiJKU1N0b3JhZ2VSZXNvdXJjZXNFeGNlZWRlZEVyciIsImNvZGUiOjUwMCwiZXJyb3JfY29kZSI6MTAwNDcsImRlc2NyaXB0aW9uIjoiaW5zdWZmaWNpZW50IHN0b3JhZ2UgcmVzb3VyY2VzIGF2YWlsYWJsZSIsImNvbW1lbnQiOiIiLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IkVyclN0b3JhZ2VSZXNvdXJjZXNFeGNlZWRlZCJ9LHsiY29uc3RhbnQiOiJKU1N0cmVhbUFzc2lnbm1lbnRFcnJGIiwiY29kZSI6NTAwLCJlcnJvcl9jb2RlIjoxMDA0OCwiZGVzY3JpcHRpb24iOiJ7ZXJyfSIsImNvbW1lbnQiOiJHZW5lcmljIHN0cmVhbSBhc3NpZ25tZW50IGVycm9yIHN0cmluZyIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTU3RyZWFtQ3JlYXRlRXJyRiIsImNvZGUiOjUwMCwiZXJyb3JfY29kZSI6MTAwNDksImRlc2NyaXB0aW9uIjoie2Vycn0iLCJjb21tZW50IjoiR2VuZXJpYyBzdHJlYW0gY3JlYXRpb24gZXJyb3Igc3RyaW5nIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNTdHJlYW1EZWxldGVFcnJGIiwiY29kZSI6NTAwLCJlcnJvcl9jb2RlIjoxMDA1MCwiZGVzY3JpcHRpb24iOiJ7ZXJyfSIsImNvbW1lbnQiOiJHZW5lcmFsIHN0cmVhbSBkZWxldGlvbiBlcnJvciBzdHJpbmciLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IiJ9LHsiY29uc3RhbnQiOiJKU1N0cmVhbUV4dGVybmFsQXBpT3ZlcmxhcEVyckYiLCJjb2RlIjo0MDAsImVycm9yX2NvZGUiOjEwMDIxLCJkZXNjcmlwdGlvbiI6InN0cmVhbSBleHRlcm5hbCBhcGkgcHJlZml4IHtwcmVmaXh9IG11c3Qgbm90IG92ZXJsYXAgd2l0aCB7c3ViamVjdH0iLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNTdHJlYW1FeHRlcm5hbERlbFByZWZpeE92ZXJsYXBzRXJyRiIsImNvZGUiOjQwMCwiZXJyb3JfY29kZSI6MTAwMjIsImRlc2NyaXB0aW9uIjoic3RyZWFtIGV4dGVybmFsIGRlbGl2ZXJ5IHByZWZpeCB7cHJlZml4fSBvdmVybGFwcyB3aXRoIHN0cmVhbSBzdWJqZWN0IHtzdWJqZWN0fSIsImNvbW1lbnQiOiIiLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IiJ9LHsiY29uc3RhbnQiOiJKU1N0cmVhbUdlbmVyYWxFcnJvckYiLCJjb2RlIjo1MDAsImVycm9yX2NvZGUiOjEwMDUxLCJkZXNjcmlwdGlvbiI6IkdlbmVyYWwgc3RyZWFtIGZhaWx1cmUgc3RyaW5nIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTU3RyZWFtSW52YWxpZENvbmZpZ0YiLCJjb2RlIjo1MDAsImVycm9yX2NvZGUiOjEwMDUyLCJkZXNjcmlwdGlvbiI6IntlcnJ9IiwiY29tbWVudCI6IlN0cmVhbSBjb25maWd1cmF0aW9uIHZhbGlkYXRpb24gZXJyb3Igc3RyaW5nIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNTdHJlYW1JbnZhbGlkRXh0ZXJuYWxEZWxpdmVyeVN1YmpFcnJGIiwiY29kZSI6NDAwLCJlcnJvcl9jb2RlIjoxMDAyNCwiZGVzY3JpcHRpb24iOiJzdHJlYW0gZXh0ZXJuYWwgZGVsaXZlcnkgcHJlZml4IHtwcmVmaXh9IG11c3Qgbm90IGNvbnRhaW4gd2lsZGNhcmRzIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTU3RyZWFtTGltaXRzRXJyRiIsImNvZGUiOjUwMCwiZXJyb3JfY29kZSI6MTAwNTMsImRlc2NyaXB0aW9uIjoie2Vycn0iLCJjb21tZW50IjoiR2VuZXJhbCBzdHJlYW0gbGltaXRzIGV4Y2VlZGVkIGVycm9yIHN0cmluZyIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTU3RyZWFtTWVzc2FnZUV4Y2VlZHNNYXhpbXVtRXJyIiwiY29kZSI6NDAwLCJlcnJvcl9jb2RlIjoxMDA1NCwiZGVzY3JpcHRpb24iOiJtZXNzYWdlIHNpemUgZXhjZWVkcyBtYXhpbXVtIGFsbG93ZWQiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNTdHJlYW1NaXJyb3JOb3RVcGRhdGFibGVFcnIiLCJjb2RlIjo0MDAsImVycm9yX2NvZGUiOjEwMDU1LCJkZXNjcmlwdGlvbiI6Ik1pcnJvciBjb25maWd1cmF0aW9uIGNhbiBub3QgYmUgdXBkYXRlZCIsImNvbW1lbnQiOiIiLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IiJ9LHsiY29uc3RhbnQiOiJKU1N0cmVhbU1pc21hdGNoRXJyIiwiY29kZSI6NDAwLCJlcnJvcl9jb2RlIjoxMDA1NiwiZGVzY3JpcHRpb24iOiJzdHJlYW0gbmFtZSBpbiBzdWJqZWN0IGRvZXMgbm90IG1hdGNoIHJlcXVlc3QiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNTdHJlYW1Nc2dEZWxldGVGYWlsZWRGIiwiY29kZSI6NTAwLCJlcnJvcl9jb2RlIjoxMDA1NywiZGVzY3JpcHRpb24iOiJ7ZXJyfSIsImNvbW1lbnQiOiJHZW5lcmljIG1lc3NhZ2UgZGVsZXRpb24gZmFpbHVyZSBlcnJvciBzdHJpbmciLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IiJ9LHsiY29uc3RhbnQiOiJKU1N0cmVhbU5hbWVFeGlzdEVyciIsImNvZGUiOjQwMCwiZXJyb3JfY29kZSI6MTAwNTgsImRlc2NyaXB0aW9uIjoic3RyZWFtIG5hbWUgYWxyZWFkeSBpbiB1c2UiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiJFcnJKZXRTdHJlYW1TdHJlYW1BbHJlYWR5VXNlZCJ9LHsiY29uc3RhbnQiOiJKU1N0cmVhbU5vdEZvdW5kRXJyIiwiY29kZSI6NDA0LCJlcnJvcl9jb2RlIjoxMDA1OSwiZGVzY3JpcHRpb24iOiJzdHJlYW0gbm90IGZvdW5kIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiRXJySmV0U3RyZWFtU3RyZWFtTm90Rm91bmQifSx7ImNvbnN0YW50IjoiSlNTdHJlYW1Ob3RNYXRjaEVyciIsImNvZGUiOjQwMCwiZXJyb3JfY29kZSI6MTAwNjAsImRlc2NyaXB0aW9uIjoiZXhwZWN0ZWQgc3RyZWFtIGRvZXMgbm90IG1hdGNoIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTU3RyZWFtUmVwbGljYXNOb3RTdXBwb3J0ZWRFcnIiLCJjb2RlIjo1MDAsImVycm9yX2NvZGUiOjEwMDc0LCJkZXNjcmlwdGlvbiI6InJlcGxpY2FzIFx1MDAzZSAxIG5vdCBzdXBwb3J0ZWQgaW4gbm9uLWNsdXN0ZXJlZCBtb2RlIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiRXJyUmVwbGljYXNOb3RTdXBwb3J0ZWQifSx7ImNvbnN0YW50IjoiSlNTdHJlYW1SZXBsaWNhc05vdFVwZGF0YWJsZUVyciIsImNvZGUiOjQwMCwiZXJyb3JfY29kZSI6MTAwNjEsImRlc2NyaXB0aW9uIjoiUmVwbGljYXMgY29uZmlndXJhdGlvbiBjYW4gbm90IGJlIHVwZGF0ZWQiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNTdHJlYW1SZXN0b3JlRXJyRiIsImNvZGUiOjUwMCwiZXJyb3JfY29kZSI6MTAwNjIsImRlc2NyaXB0aW9uIjoicmVzdG9yZSBmYWlsZWQ6IHtlcnJ9IiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTU3RyZWFtU2VxdWVuY2VOb3RNYXRjaEVyciIsImNvZGUiOjUwMywiZXJyb3JfY29kZSI6MTAwNjMsImRlc2NyaXB0aW9uIjoiZXhwZWN0ZWQgc3RyZWFtIHNlcXVlbmNlIGRvZXMgbm90IG1hdGNoIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTU3RyZWFtU25hcHNob3RFcnJGIiwiY29kZSI6NTAwLCJlcnJvcl9jb2RlIjoxMDA2NCwiZGVzY3JpcHRpb24iOiJzbmFwc2hvdCBmYWlsZWQ6IHtlcnJ9IiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTU3RyZWFtU3ViamVjdE92ZXJsYXBFcnIiLCJjb2RlIjo1MDAsImVycm9yX2NvZGUiOjEwMDY1LCJkZXNjcmlwdGlvbiI6InN1YmplY3RzIG92ZXJsYXAgd2l0aCBhbiBleGlzdGluZyBzdHJlYW0iLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNTdHJlYW1UZW1wbGF0ZUNyZWF0ZUVyckYiLCJjb2RlIjo1MDAsImVycm9yX2NvZGUiOjEwMDY2LCJkZXNjcmlwdGlvbiI6IntlcnJ9IiwiY29tbWVudCI6IkdlbmVyaWMgdGVtcGxhdGUgY3JlYXRpb24gZmFpbGVkIHN0cmluZyIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTU3RyZWFtVGVtcGxhdGVEZWxldGVFcnJGIiwiY29kZSI6NTAwLCJlcnJvcl9jb2RlIjoxMDA2NywiZGVzY3JpcHRpb24iOiJ7ZXJyfSIsImNvbW1lbnQiOiJHZW5lcmljIHN0cmVhbSB0ZW1wbGF0ZSBkZWxldGlvbiBmYWlsZWQgZXJyb3Igc3RyaW5nIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNTdHJlYW1UZW1wbGF0ZU5vdEZvdW5kRXJyIiwiY29kZSI6NDA0LCJlcnJvcl9jb2RlIjoxMDA2OCwiZGVzY3JpcHRpb24iOiJ0ZW1wbGF0ZSBub3QgZm91bmQiLCJjb21tZW50IjoiIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNTdHJlYW1VcGRhdGVFcnJGIiwiY29kZSI6NTAwLCJlcnJvcl9jb2RlIjoxMDA2OSwiZGVzY3JpcHRpb24iOiJ7ZXJyfSIsImNvbW1lbnQiOiJHZW5lcmljIHN0cmVhbSB1cGRhdGUgZXJyb3Igc3RyaW5nIiwiaGVscCI6IiIsInVybCI6IiIsImRlcHJlY2F0ZXMiOiIifSx7ImNvbnN0YW50IjoiSlNTdHJlYW1Xcm9uZ0xhc3RNc2dJREVyckYiLCJjb2RlIjo0MDAsImVycm9yX2NvZGUiOjEwMDcwLCJkZXNjcmlwdGlvbiI6Indyb25nIGxhc3QgbXNnIElEOiB7aWR9IiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTU3RyZWFtV3JvbmdMYXN0U2VxdWVuY2VFcnJGIiwiY29kZSI6NDAwLCJlcnJvcl9jb2RlIjoxMDA3MSwiZGVzY3JpcHRpb24iOiJ3cm9uZyBsYXN0IHNlcXVlbmNlOiB7c2VxfSIsImNvbW1lbnQiOiIiLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IiJ9LHsiY29uc3RhbnQiOiJKU1RlbXBTdG9yYWdlRmFpbGVkRXJyIiwiY29kZSI6NTAwLCJlcnJvcl9jb2RlIjoxMDA3MiwiZGVzY3JpcHRpb24iOiJKZXRTdHJlYW0gdW5hYmxlIHRvIG9wZW4gdGVtcCBzdG9yYWdlIGZvciByZXN0b3JlIiwiY29tbWVudCI6IiIsImhlbHAiOiIiLCJ1cmwiOiIiLCJkZXByZWNhdGVzIjoiIn0seyJjb25zdGFudCI6IkpTVGVtcGxhdGVOYW1lTm90TWF0Y2hTdWJqZWN0RXJyIiwiY29kZSI6NDAwLCJlcnJvcl9jb2RlIjoxMDA3MywiZGVzY3JpcHRpb24iOiJ0ZW1wbGF0ZSBuYW1lIGluIHN1YmplY3QgZG9lcyBub3QgbWF0Y2ggcmVxdWVzdCIsImNvbW1lbnQiOiIiLCJoZWxwIjoiIiwidXJsIjoiIiwiZGVwcmVjYXRlcyI6IiJ9XQ=="
)
