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
	JSServerVersionMetadataKey  = "_nats.server.version"
	JSServerLevelMetadataKey    = "_nats.server.api_level"
)

// setStreamAssetVersionMetadata sets JetStream stream metadata, like the server version and API level.
// Given:
//   - cfg!=nil, prevCfg==nil		add stream: adds created and required metadata
//   - cfg!=nil, prevCfg!=nil		update stream: created metadata is preserved, required metadata is updated
//
// Any dynamic metadata is removed, it must not be stored and only be added for responses.
func setStreamAssetVersionMetadata(cfg *StreamConfig, prevCfg *StreamConfig) {
	if cfg.Metadata == nil {
		cfg.Metadata = make(map[string]string)
	} else {
		deleteDynamicMetadata(cfg.Metadata)
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

// includeDynamicStreamAssetVersionMetadata adds dynamic fields into the (copied) metadata.
func includeDynamicStreamAssetVersionMetadata(cfg StreamConfig) StreamConfig {
	newCfg := cfg
	newCfg.Metadata = make(map[string]string)
	for key, value := range cfg.Metadata {
		newCfg.Metadata[key] = value
	}
	newCfg.Metadata[JSServerVersionMetadataKey] = VERSION
	newCfg.Metadata[JSServerLevelMetadataKey] = JSApiLevel
	return newCfg
}

// setConsumerAssetVersionMetadata sets JetStream consumer metadata, like the server version and API level.
// Given:
//   - cfg!=nil, prevCfg==nil		add consumer: adds created and required metadata
//   - cfg!=nil, prevCfg!=nil		update consumer: created metadata is preserved, required metadata is updated
//
// Any dynamic metadata is removed, it must not be stored and only be added for responses.
func setConsumerAssetVersionMetadata(cfg *ConsumerConfig, prevCfg *ConsumerConfig) {
	if cfg.Metadata == nil {
		cfg.Metadata = make(map[string]string)
	} else {
		deleteDynamicMetadata(cfg.Metadata)
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

// includeDynamicConsumerAssetVersionMetadata adds dynamic fields into the (copied) metadata.
func includeDynamicConsumerAssetVersionMetadata(cfg *ConsumerConfig) *ConsumerConfig {
	newCfg := *cfg
	newCfg.Metadata = make(map[string]string)
	for key, value := range cfg.Metadata {
		newCfg.Metadata[key] = value
	}
	newCfg.Metadata[JSServerVersionMetadataKey] = VERSION
	newCfg.Metadata[JSServerLevelMetadataKey] = JSApiLevel
	return &newCfg
}

// includeDynamicConsumerInfoVersionMetadata adds dynamic fields into the (copied) metadata.
func includeDynamicConsumerInfoVersionMetadata(info *ConsumerInfo) *ConsumerInfo {
	if info == nil {
		return nil
	}

	newInfo := *info
	cfg := includeDynamicConsumerAssetVersionMetadata(info.Config)
	newInfo.Config = cfg
	return &newInfo
}

// copyConsumerAssetVersionMetadata copies versioning fields from metadata of prevCfg into cfg.
// Removes versioning fields if no previous metadata, updates if set, and removes fields if it doesn't exist in prevCfg.
// Any dynamic metadata is removed, it must not be stored and only be added for responses.
//
// Note: useful when doing equality checks on cfg and prevCfg, but ignoring any versioning metadata differences.
// MUST be followed up with a call to setConsumerAssetVersionMetadata to fix potentially lost metadata.
func copyConsumerAssetVersionMetadata(cfg *ConsumerConfig, prevCfg *ConsumerConfig) {
	if cfg.Metadata != nil {
		deleteDynamicMetadata(cfg.Metadata)
	}

	// Remove fields when no previous metadata.
	if prevCfg == nil || prevCfg.Metadata == nil {
		if cfg.Metadata != nil {
			delete(cfg.Metadata, JSCreatedVersionMetadataKey)
			delete(cfg.Metadata, JSCreatedLevelMetadataKey)
			delete(cfg.Metadata, JSRequiredLevelMetadataKey)
			if len(cfg.Metadata) == 0 {
				cfg.Metadata = nil
			}
		}
		return
	}

	// Set if exists, delete otherwise.
	setOrDeleteInMetadata(cfg, prevCfg, JSCreatedVersionMetadataKey)
	setOrDeleteInMetadata(cfg, prevCfg, JSCreatedLevelMetadataKey)
	setOrDeleteInMetadata(cfg, prevCfg, JSRequiredLevelMetadataKey)
}

// setOrDeleteInMetadata sets field with key/value in metadata of cfg if set, deletes otherwise.
func setOrDeleteInMetadata(cfg *ConsumerConfig, prevCfg *ConsumerConfig, key string) {
	if value, ok := prevCfg.Metadata[key]; ok {
		if cfg.Metadata == nil {
			cfg.Metadata = make(map[string]string)
		}
		cfg.Metadata[key] = value
	} else {
		delete(cfg.Metadata, key)
	}
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

// deleteDynamicMetadata deletes dynamic fields from the metadata.
func deleteDynamicMetadata(metadata map[string]string) {
	delete(metadata, JSServerVersionMetadataKey)
	delete(metadata, JSServerLevelMetadataKey)
}
