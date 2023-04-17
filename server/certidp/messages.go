// Copyright 2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package certidp

var (
	// Returned errors
	ErrIllegalPeerOptsConfig              = "expected map to define OCSP peer options, got [%T]"
	ErrIllegalCacheOptsConfig             = "expected map to define OCSP peer cache options, got [%T]"
	ErrParsingPeerOptFieldGeneric         = "error parsing tls peer config, unknown field [%q]"
	ErrParsingPeerOptFieldTypeConversion  = "error parsing tls peer config, conversion error: %s"
	ErrParsingCacheOptFieldTypeConversion = "error parsing OCSP peer cache config, conversion error: %s"
	ErrUnableToPlugTLSEmptyConfig         = "unable to plug TLS verify connection, config is nil"
	ErrMTLSRequired                       = "OCSP peer verification for client connections requires TLS verify (mTLS) to be enabled"
	ErrUnableToPlugTLSClient              = "unable to register client OCSP verification"
	ErrUnableToPlugTLSServer              = "unable to register server OCSP verification"
	ErrCannotWriteCompressed              = "error writing to compression writer: %w"
	ErrCannotReadCompressed               = "error reading compression reader: %w"
	ErrTruncatedWrite                     = "short write on body (%d != %d)"
	ErrCannotCloseWriter                  = "error closing compression writer: %w"
	ErrParsingCacheOptFieldGeneric        = "error parsing OCSP peer cache config, unknown field [%q]"
	ErrUnknownCacheType                   = "error parsing OCSP peer cache config, unknown type [%s]"
	ErrInvalidChainlink                   = "invalid chain link"
	ErrBadResponderHTTPStatus             = "bad OCSP responder http status: [%d]"
	ErrNoAvailOCSPServers                 = "no available OCSP servers"
	ErrFailedWithAllRequests              = "exhausted OCSP responders: %w"

	// Direct logged errors
	ErrLoadCacheFail          = "Unable to load OCSP peer cache: %s"
	ErrSaveCacheFail          = "Unable to save OCSP peer cache: %s"
	ErrBadCacheTypeConfig     = "Unimplemented OCSP peer cache type [%v]"
	ErrResponseCompressFail   = "Unable to compress OCSP response for key [%s]: %s"
	ErrResponseDecompressFail = "Unable to decompress OCSP response for key [%s]: %s"
	ErrPeerEmptyNoEvent       = "Peer certificate is nil, cannot send OCSP peer reject event"
	ErrPeerEmptyAutoReject    = "Peer certificate is nil, rejecting OCSP peer"

	// Debug information
	DbgPlugTLSForKind        = "Plugging TLS OCSP peer for [%s]"
	DbgNumServerChains       = "Peer OCSP enabled: %d TLS server chain(s) will be evaluated"
	DbgNumClientChains       = "Peer OCSP enabled: %d TLS client chain(s) will be evaluated"
	DbgLinksInChain          = "Chain [%d]: %d total link(s)"
	DbgSelfSignedValid       = "Chain [%d] is self-signed, thus peer is valid"
	DbgValidNonOCSPChain     = "Chain [%d] has no OCSP eligible links, thus peer is valid"
	DbgChainIsOCSPEligible   = "Chain [%d] has %d OCSP eligible link(s)"
	DbgChainIsOCSPValid      = "Chain [%d] is OCSP valid for all eligible links, thus peer is valid"
	DbgNoOCSPValidChains     = "No OCSP valid chains, thus peer is invalid"
	DbgCheckingCacheForCert  = "Checking OCSP peer cache for [%s], key [%s]"
	DbgCurrentResponseCached = "Cached OCSP response is current, status [%s]"
	DbgExpiredResponseCached = "Cached OCSP response is expired, status [%s]"
	DbgOCSPValidPeerLink     = "OCSP verify pass for [%s]"
	DbgCachingResponse       = "Caching OCSP response for [%s], key [%s]"
	DbgAchievedCompression   = "OCSP response compression ratio: [%f]"
	DbgCacheHit              = "OCSP peer cache hit for key [%s]"
	DbgCacheMiss             = "OCSP peer cache miss for key [%s]"
	DbgPreservedRevocation   = "Revoked OCSP response for key [%s] preserved by cache policy"
	DbgDeletingCacheResponse = "Deleting OCSP peer cached response for key [%s]"
	DbgStartingCache         = "Starting OCSP peer cache"
	DbgStoppingCache         = "Stopping OCSP peer cache"
	DbgLoadingCache          = "Loading OCSP peer cache [%s]"
	DbgNoCacheFound          = "No OCSP peer cache found, starting with empty cache"
	DbgSavingCache           = "Saving OCSP peer cache [%s]"
	DbgCacheSaved            = "Saved OCSP peer cache successfully (%d bytes)"
	DbgMakingCARequest       = "Trying OCSP responder url [%s]"
	DbgResponseExpired       = "OCSP response NextUpdate [%s] is before now [%s] with clockskew [%s]"
	DbgResponseTTLExpired    = "OCSP response cache expiry [%s] is before now [%s] with clockskew [%s]"
	DbgResponseFutureDated   = "OCSP response ThisUpdate [%s] is before now [%s] with clockskew [%s]"
	DbgCacheSaveTimerExpired = "OCSP peer cache save timer expired"
	DbgCacheDirtySave        = "OCSP peer cache is dirty, saving"

	// Returned to peer as TLS reject reason
	MsgTLSClientRejectConnection = "client not OCSP valid"
	MsgTLSServerRejectConnection = "server not OCSP valid"

	// Expected runtime errors (direct logged)
	ErrCAResponderCalloutFail  = "Attempt to obtain OCSP response from CA responder for [%s] failed: %s"
	ErrNewCAResponseNotCurrent = "New OCSP CA response obtained for [%s] but not current"
	ErrCAResponseParseFailed   = "Could not parse OCSP CA response for [%s]: %s"
	ErrOCSPInvalidPeerLink     = "OCSP verify fail for [%s] with CA status [%s]"

	// Policy override warnings (direct logged)
	MsgAllowWhenCAUnreachableOccurred             = "Failed to obtain OCSP CA response for [%s] but AllowWhenCAUnreachable set; no cached revocation so allowing"
	MsgAllowWhenCAUnreachableOccurredCachedRevoke = "Failed to obtain OCSP CA response for [%s] but AllowWhenCAUnreachable set; cached revocation exists so rejecting"
	MsgAllowWarnOnlyOccurred                      = "OCSP verify fail for [%s] but WarnOnly is true so allowing"

	// Info (direct logged)
	MsgCacheOnline  = "OCSP peer cache online, type [%s]"
	MsgCacheOffline = "OCSP peer cache offline, type [%s]"

	// OCSP cert invalid reasons (debug and event reasons)
	MsgFailedOCSPResponseFetch       = "Failed OCSP response fetch"
	MsgOCSPResponseNotEffective      = "OCSP response not in effectivity window"
	MsgFailedOCSPResponseParse       = "Failed OCSP response parse"
	MsgOCSPResponseInvalidStatus     = "Invalid OCSP response status: %s"
	MsgOCSPResponseDelegationInvalid = "Invalid OCSP response delegation: %s"
	MsgCachedOCSPResponseInvalid     = "Invalid cached OCSP response for [%s] with fingerprint [%s]"
)
