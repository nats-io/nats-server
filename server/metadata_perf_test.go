package server

import (
	"strconv"
	"testing"
)

func TestMetadataCorrectness(t *testing.T) {
	cfg := &StreamConfig{
		Metadata: map[string]string{"user-key": "user-val"},
	}
	newCfg := setDynamicStreamMetadata(cfg)
	if newCfg.Metadata["user-key"] != "user-val" {
		t.Errorf("Expected user-key to be preserved")
	}
	if newCfg.Metadata[JSServerVersionMetadataKey] != VERSION {
		t.Errorf("Expected version %s, got %s", VERSION, newCfg.Metadata[JSServerVersionMetadataKey])
	}
	// Use literal logic here so it compiles on both versions
	expectedLevel := strconv.Itoa(JSApiLevel)
	if newCfg.Metadata[JSServerLevelMetadataKey] != expectedLevel {
		t.Errorf("Expected level %s, got %s", expectedLevel, newCfg.Metadata[JSServerLevelMetadataKey])
	}
}

func BenchmarkMetadataAllocations(b *testing.B) {
	cfg := &StreamConfig{
		Metadata: map[string]string{
			"key1": "val1", "key2": "val2", "key3": "val3",
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = setDynamicStreamMetadata(cfg)
	}
}
