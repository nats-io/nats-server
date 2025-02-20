// Copyright 2024-2025 The NATS Authors
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

import "strconv"

const (
	// JSApiLevel is the maximum supported JetStream API level for this server.
	JSApiLevel int = 1

	JSRequiredLevelMetadataKey = "_nats.req.level"
	JSServerVersionMetadataKey = "_nats.ver"
	JSServerLevelMetadataKey   = "_nats.level"
)

// setStaticStreamMetadata sets JetStream stream metadata, like the server version and API level.
// Any dynamic metadata is removed, it must not be stored and only be added for responses.
func setStaticStreamMetadata(cfg *StreamConfig) {
	if cfg.Metadata == nil {
		cfg.Metadata = make(map[string]string)
	} else {
		deleteDynamicMetadata(cfg.Metadata)
	}

	var requiredApiLevel int
	requires := func(level int) {
		if level > requiredApiLevel {
			requiredApiLevel = level
		}
	}

	// TTLs were added in v2.11 and require API level 1.
	if cfg.AllowMsgTTL || cfg.SubjectDeleteMarkerTTL > 0 {
		requires(1)
	}

	cfg.Metadata[JSRequiredLevelMetadataKey] = strconv.Itoa(requiredApiLevel)
}

// setDynamicStreamMetadata adds dynamic fields into the (copied) metadata.
func setDynamicStreamMetadata(cfg *StreamConfig) *StreamConfig {
	newCfg := *cfg
	newCfg.Metadata = make(map[string]string)
	for key, value := range cfg.Metadata {
		newCfg.Metadata[key] = value
	}
	newCfg.Metadata[JSServerVersionMetadataKey] = VERSION
	newCfg.Metadata[JSServerLevelMetadataKey] = strconv.Itoa(JSApiLevel)
	return &newCfg
}

// copyConsumerMetadata copies versioning fields from metadata of prevCfg into cfg.
// Removes versioning fields if no previous metadata, updates if set, and removes fields if it doesn't exist in prevCfg.
// Any dynamic metadata is removed, it must not be stored and only be added for responses.
//
// Note: useful when doing equality checks on cfg and prevCfg, but ignoring any versioning metadata differences.
func copyStreamMetadata(cfg *StreamConfig, prevCfg *StreamConfig) {
	if cfg.Metadata != nil {
		deleteDynamicMetadata(cfg.Metadata)
	}
	setOrDeleteInStreamMetadata(cfg, prevCfg, JSRequiredLevelMetadataKey)
}

// setOrDeleteInConsumerMetadata sets field with key/value in metadata of cfg if set, deletes otherwise.
func setOrDeleteInStreamMetadata(cfg *StreamConfig, prevCfg *StreamConfig, key string) {
	if prevCfg != nil && prevCfg.Metadata != nil {
		if value, ok := prevCfg.Metadata[key]; ok {
			if cfg.Metadata == nil {
				cfg.Metadata = make(map[string]string)
			}
			cfg.Metadata[key] = value
			return
		}
	}
	delete(cfg.Metadata, key)
	if len(cfg.Metadata) == 0 {
		cfg.Metadata = nil
	}
}

// setStaticConsumerMetadata sets JetStream consumer metadata, like the server version and API level.
// Any dynamic metadata is removed, it must not be stored and only be added for responses.
func setStaticConsumerMetadata(cfg *ConsumerConfig) {
	if cfg.Metadata == nil {
		cfg.Metadata = make(map[string]string)
	} else {
		deleteDynamicMetadata(cfg.Metadata)
	}

	var requiredApiLevel int
	requires := func(level int) {
		if level > requiredApiLevel {
			requiredApiLevel = level
		}
	}

	// Added in 2.11, absent | zero is the feature is not used.
	// one could be stricter and say even if its set but the time
	// has already passed it is also not needed to restore the consumer
	if cfg.PauseUntil != nil && !cfg.PauseUntil.IsZero() {
		requires(1)
	}

	if cfg.PriorityPolicy != PriorityNone || cfg.PinnedTTL != 0 || len(cfg.PriorityGroups) > 0 {
		requires(1)
	}

	cfg.Metadata[JSRequiredLevelMetadataKey] = strconv.Itoa(requiredApiLevel)
}

// setDynamicConsumerMetadata adds dynamic fields into the (copied) metadata.
func setDynamicConsumerMetadata(cfg *ConsumerConfig) *ConsumerConfig {
	newCfg := *cfg
	newCfg.Metadata = make(map[string]string)
	for key, value := range cfg.Metadata {
		newCfg.Metadata[key] = value
	}
	newCfg.Metadata[JSServerVersionMetadataKey] = VERSION
	newCfg.Metadata[JSServerLevelMetadataKey] = strconv.Itoa(JSApiLevel)
	return &newCfg
}

// setDynamicConsumerInfoMetadata adds dynamic fields into the (copied) metadata.
func setDynamicConsumerInfoMetadata(info *ConsumerInfo) *ConsumerInfo {
	if info == nil {
		return nil
	}

	newInfo := *info
	cfg := setDynamicConsumerMetadata(info.Config)
	newInfo.Config = cfg
	return &newInfo
}

// copyConsumerMetadata copies versioning fields from metadata of prevCfg into cfg.
// Removes versioning fields if no previous metadata, updates if set, and removes fields if it doesn't exist in prevCfg.
// Any dynamic metadata is removed, it must not be stored and only be added for responses.
//
// Note: useful when doing equality checks on cfg and prevCfg, but ignoring any versioning metadata differences.
func copyConsumerMetadata(cfg *ConsumerConfig, prevCfg *ConsumerConfig) {
	if cfg.Metadata != nil {
		deleteDynamicMetadata(cfg.Metadata)
	}
	setOrDeleteInConsumerMetadata(cfg, prevCfg, JSRequiredLevelMetadataKey)
}

// setOrDeleteInConsumerMetadata sets field with key/value in metadata of cfg if set, deletes otherwise.
func setOrDeleteInConsumerMetadata(cfg *ConsumerConfig, prevCfg *ConsumerConfig, key string) {
	if prevCfg != nil && prevCfg.Metadata != nil {
		if value, ok := prevCfg.Metadata[key]; ok {
			if cfg.Metadata == nil {
				cfg.Metadata = make(map[string]string)
			}
			cfg.Metadata[key] = value
			return
		}
	}
	delete(cfg.Metadata, key)
	if len(cfg.Metadata) == 0 {
		cfg.Metadata = nil
	}
}

// deleteDynamicMetadata deletes dynamic fields from the metadata.
func deleteDynamicMetadata(metadata map[string]string) {
	delete(metadata, JSServerVersionMetadataKey)
	delete(metadata, JSServerLevelMetadataKey)
}
