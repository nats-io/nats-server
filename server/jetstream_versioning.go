// Copyright 2024 The NATS Authors
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

package server

const (
	// JSApiLevel is the maximum supported JetStream API level for this server.
	JSApiLevel = "1"

	JSCreatedVersionMetadataKey = "_nats.created.server.version"
	JSCreatedLevelMetadataKey   = "_nats.created.server.api_level"
	JSRequiredLevelMetadataKey  = "_nats.server.require.api_level"
)

// setStreamAssetVersionMetadata sets JetStream stream metadata, like the server version and API level.
// Given:
//   - cfg!=nil, prevCfg==nil		add stream: adds created and required metadata
//   - cfg!=nil, prevCfg!=nil		update stream: created metadata is preserved, required metadata is updated
func setStreamAssetVersionMetadata(cfg *StreamConfig, prevCfg *StreamConfig) {
	if cfg.Metadata == nil {
		cfg.Metadata = make(map[string]string)
	}

	var prevMetadata map[string]string
	if prevCfg != nil {
		prevMetadata = prevCfg.Metadata
		if prevMetadata == nil {
			// Initialize to empty to indicate we had a previous config but metadata was missing.
			prevMetadata = make(map[string]string)
		}
	}
	preserveAssetCreatedVersionMetadata(cfg.Metadata, prevMetadata)

	requiredApiLevel := "0"
	cfg.Metadata[JSRequiredLevelMetadataKey] = requiredApiLevel
}

// setConsumerAssetVersionMetadata sets JetStream consumer metadata, like the server version and API level.
// Given:
//   - cfg!=nil, prevCfg==nil		add consumer: adds created and required metadata
//   - cfg!=nil, prevCfg!=nil		update consumer: created metadata is preserved, required metadata is updated
func setConsumerAssetVersionMetadata(cfg *ConsumerConfig, prevCfg *ConsumerConfig) {
	if cfg.Metadata == nil {
		cfg.Metadata = make(map[string]string)
	}

	var prevMetadata map[string]string
	if prevCfg != nil {
		prevMetadata = prevCfg.Metadata
		if prevMetadata == nil {
			// Initialize to empty to indicate we had a previous config but metadata was missing.
			prevMetadata = make(map[string]string)
		}
	}
	preserveAssetCreatedVersionMetadata(cfg.Metadata, prevMetadata)

	requiredApiLevel := "0"

	// Added in 2.11, absent | zero is the feature is not used.
	// one could be stricter and say even if its set but the time
	// has already passed it is also not needed to restore the consumer
	if cfg.PauseUntil != nil && !cfg.PauseUntil.IsZero() {
		requiredApiLevel = "1"
	}

	cfg.Metadata[JSRequiredLevelMetadataKey] = requiredApiLevel
}

// preserveAssetCreatedVersionMetadata sets metadata to contain which version and API level the asset was created on.
// Preserves previous metadata, if not set it initializes versions for the metadata.
func preserveAssetCreatedVersionMetadata(metadata, prevMetadata map[string]string) {
	if prevMetadata == nil {
		metadata[JSCreatedVersionMetadataKey] = VERSION
		metadata[JSCreatedLevelMetadataKey] = JSApiLevel
		return
	}

	// Preserve previous metadata if it was set, but delete if not since it could be user-provided.
	if v := prevMetadata[JSCreatedVersionMetadataKey]; v != _EMPTY_ {
		metadata[JSCreatedVersionMetadataKey] = v
	} else {
		delete(metadata, JSCreatedVersionMetadataKey)
	}
	if v := prevMetadata[JSCreatedLevelMetadataKey]; v != _EMPTY_ {
		metadata[JSCreatedLevelMetadataKey] = v
	} else {
		delete(metadata, JSCreatedLevelMetadataKey)
	}
}
