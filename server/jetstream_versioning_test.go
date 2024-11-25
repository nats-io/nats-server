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

//go:build !skip_js_tests
// +build !skip_js_tests

package server

import (
	"archive/tar"
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/klauspost/compress/s2"
	"github.com/nats-io/nats.go"
)

func metadataAllSet(featureLevel string) map[string]string {
	return map[string]string{
		JSCreatedVersionMetadataKey: VERSION,
		JSCreatedLevelMetadataKey:   strconv.Itoa(JSApiLevel),
		JSRequiredLevelMetadataKey:  featureLevel,
	}
}

func metadataPrevious() map[string]string {
	return map[string]string{
		JSCreatedVersionMetadataKey: "previous-version",
		JSCreatedLevelMetadataKey:   "previous-level",
		JSRequiredLevelMetadataKey:  "previous-level",
	}
}

func metadataUpdatedPrevious(featureLevel string) map[string]string {
	return map[string]string{
		JSCreatedVersionMetadataKey: "previous-version",
		JSCreatedLevelMetadataKey:   "previous-level",
		JSRequiredLevelMetadataKey:  featureLevel,
	}
}

func metadataOnlyRequired() map[string]string {
	return map[string]string{
		JSRequiredLevelMetadataKey: "0",
	}
}

func TestJetStreamSetStaticStreamMetadata(t *testing.T) {
	for _, test := range []struct {
		desc             string
		cfg              *StreamConfig
		prev             *StreamConfig
		expectedMetadata map[string]string
	}{
		{
			desc:             "create",
			cfg:              &StreamConfig{},
			prev:             nil,
			expectedMetadata: metadataAllSet("0"),
		},
		{
			desc:             "create/overwrite-user-provided",
			cfg:              &StreamConfig{Metadata: metadataPrevious()},
			prev:             nil,
			expectedMetadata: metadataAllSet("0"),
		},
		{
			desc:             "update",
			cfg:              &StreamConfig{},
			prev:             &StreamConfig{Metadata: metadataPrevious()},
			expectedMetadata: metadataUpdatedPrevious("0"),
		},
		{
			desc:             "update/empty-prev-metadata",
			cfg:              &StreamConfig{},
			prev:             &StreamConfig{},
			expectedMetadata: metadataOnlyRequired(),
		},
		{
			desc:             "update/empty-prev-metadata/delete-user-provided",
			cfg:              &StreamConfig{Metadata: metadataPrevious()},
			prev:             &StreamConfig{},
			expectedMetadata: metadataOnlyRequired(),
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			setStaticStreamMetadata(test.cfg, test.prev)
			require_Equal(t, test.cfg.Metadata[JSCreatedVersionMetadataKey], test.expectedMetadata[JSCreatedVersionMetadataKey])
			require_Equal(t, test.cfg.Metadata[JSCreatedLevelMetadataKey], test.expectedMetadata[JSCreatedLevelMetadataKey])
			require_Equal(t, test.cfg.Metadata[JSRequiredLevelMetadataKey], test.expectedMetadata[JSRequiredLevelMetadataKey])
		})
	}
}

func TestJetStreamSetStaticStreamMetadataRemoveDynamicFields(t *testing.T) {
	dynamicMetadata := func() map[string]string {
		return map[string]string{
			JSServerVersionMetadataKey: "dynamic-version",
			JSServerLevelMetadataKey:   "dynamic-version",
		}
	}

	cfg := StreamConfig{Metadata: dynamicMetadata()}
	setStaticStreamMetadata(&cfg, nil)
	require_True(t, reflect.DeepEqual(cfg.Metadata, metadataAllSet("0")))

	cfg = StreamConfig{Metadata: dynamicMetadata()}
	prevCfg := StreamConfig{Metadata: metadataAllSet("0")}
	setStaticStreamMetadata(&cfg, &prevCfg)
	require_True(t, reflect.DeepEqual(cfg.Metadata, metadataAllSet("0")))
}

func TestJetStreamSetDynamicStreamMetadata(t *testing.T) {
	cfg := StreamConfig{Metadata: metadataAllSet("0")}
	newCfg := setDynamicStreamMetadata(&cfg)

	// Only new metadata must contain dynamic fields.
	metadata := metadataAllSet("0")
	require_True(t, reflect.DeepEqual(cfg.Metadata, metadata))
	metadata[JSServerVersionMetadataKey] = VERSION
	metadata[JSServerLevelMetadataKey] = strconv.Itoa(JSApiLevel)
	require_True(t, reflect.DeepEqual(newCfg.Metadata, metadata))
}

func TestJetStreamSetStaticConsumerMetadata(t *testing.T) {
	pauseUntil := time.Unix(0, 0)
	pauseUntilZero := time.Time{}
	for _, test := range []struct {
		desc             string
		cfg              *ConsumerConfig
		prev             *ConsumerConfig
		expectedMetadata map[string]string
	}{
		{
			desc:             "create",
			cfg:              &ConsumerConfig{},
			prev:             nil,
			expectedMetadata: metadataAllSet("0"),
		},
		{
			desc:             "create/PauseUntil/zero",
			cfg:              &ConsumerConfig{PauseUntil: &pauseUntilZero},
			prev:             nil,
			expectedMetadata: metadataAllSet("0"),
		},
		{
			desc:             "create/PauseUntil",
			cfg:              &ConsumerConfig{PauseUntil: &pauseUntil},
			prev:             nil,
			expectedMetadata: metadataAllSet("1"),
		},
		{
			desc:             "create/overwrite-user-provided",
			cfg:              &ConsumerConfig{Metadata: metadataPrevious()},
			prev:             nil,
			expectedMetadata: metadataAllSet("0"),
		},
		{
			desc:             "update",
			cfg:              &ConsumerConfig{},
			prev:             &ConsumerConfig{Metadata: metadataPrevious()},
			expectedMetadata: metadataUpdatedPrevious("0"),
		},
		{
			desc:             "create/PauseUntil/zero",
			cfg:              &ConsumerConfig{PauseUntil: &pauseUntilZero},
			prev:             &ConsumerConfig{Metadata: metadataPrevious()},
			expectedMetadata: metadataUpdatedPrevious("0"),
		},
		{
			desc:             "update/PauseUntil",
			cfg:              &ConsumerConfig{PauseUntil: &pauseUntil},
			prev:             &ConsumerConfig{Metadata: metadataPrevious()},
			expectedMetadata: metadataUpdatedPrevious("1"),
		},
		{
			desc:             "create/Pinned",
			cfg:              &ConsumerConfig{PriorityPolicy: PriorityPinnedClient, PriorityGroups: []string{"a"}},
			prev:             &ConsumerConfig{Metadata: metadataPrevious()},
			expectedMetadata: metadataUpdatedPrevious("1"),
		},
		{
			desc:             "update/empty-prev-metadata",
			cfg:              &ConsumerConfig{},
			prev:             &ConsumerConfig{},
			expectedMetadata: metadataOnlyRequired(),
		},
		{
			desc:             "update/empty-prev-metadata/delete-user-provided",
			cfg:              &ConsumerConfig{Metadata: metadataPrevious()},
			prev:             &ConsumerConfig{},
			expectedMetadata: metadataOnlyRequired(),
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			setStaticConsumerMetadata(test.cfg, test.prev)
			require_Equal(t, test.cfg.Metadata[JSCreatedVersionMetadataKey], test.expectedMetadata[JSCreatedVersionMetadataKey])
			require_Equal(t, test.cfg.Metadata[JSCreatedLevelMetadataKey], test.expectedMetadata[JSCreatedLevelMetadataKey])
			require_Equal(t, test.cfg.Metadata[JSRequiredLevelMetadataKey], test.expectedMetadata[JSRequiredLevelMetadataKey])
		})
	}
}

func TestJetStreamSetStaticConsumerMetadataRemoveDynamicFields(t *testing.T) {
	dynamicMetadata := func() map[string]string {
		return map[string]string{
			JSServerVersionMetadataKey: "dynamic-version",
			JSServerLevelMetadataKey:   "dynamic-version",
		}
	}

	cfg := ConsumerConfig{Metadata: dynamicMetadata()}
	setStaticConsumerMetadata(&cfg, nil)
	require_True(t, reflect.DeepEqual(cfg.Metadata, metadataAllSet("0")))

	cfg = ConsumerConfig{Metadata: dynamicMetadata()}
	prevCfg := ConsumerConfig{Metadata: metadataAllSet("0")}
	setStaticConsumerMetadata(&cfg, &prevCfg)
	require_True(t, reflect.DeepEqual(cfg.Metadata, metadataAllSet("0")))
}

func TestJetStreamSetDynamicConsumerMetadata(t *testing.T) {
	cfg := ConsumerConfig{Metadata: metadataAllSet("0")}
	newCfg := setDynamicConsumerMetadata(&cfg)

	// Only new metadata must contain dynamic fields.
	metadata := metadataAllSet("0")
	require_True(t, reflect.DeepEqual(cfg.Metadata, metadata))
	metadata[JSServerVersionMetadataKey] = VERSION
	metadata[JSServerLevelMetadataKey] = strconv.Itoa(JSApiLevel)
	require_True(t, reflect.DeepEqual(newCfg.Metadata, metadata))
}

func TestJetStreamSetDynamicConsumerInfoMetadata(t *testing.T) {
	ci := ConsumerInfo{Config: &ConsumerConfig{Metadata: metadataAllSet("0")}}
	newCi := setDynamicConsumerInfoMetadata(&ci)

	// Configs should not equal, as that would mean we've overwritten the original ConsumerInfo.
	require_False(t, reflect.DeepEqual(ci, newCi))

	// Only new metadata must contain dynamic fields.
	metadata := metadataAllSet("0")
	require_True(t, reflect.DeepEqual(ci.Config.Metadata, metadata))
	metadata[JSServerVersionMetadataKey] = VERSION
	metadata[JSServerLevelMetadataKey] = strconv.Itoa(JSApiLevel)
	require_True(t, reflect.DeepEqual(newCi.Config.Metadata, metadata))
}

func TestJetStreamCopyConsumerMetadata(t *testing.T) {
	for _, test := range []struct {
		desc string
		cfg  *ConsumerConfig
		prev *ConsumerConfig
	}{
		{
			desc: "no-previous-ignore",
			cfg:  &ConsumerConfig{Metadata: metadataAllSet("-1")},
			prev: nil,
		},
		{
			desc: "nil-previous-metadata-ignore",
			cfg:  &ConsumerConfig{Metadata: metadataAllSet("-1")},
			prev: &ConsumerConfig{Metadata: nil},
		},
		{
			desc: "nil-current-metadata-ignore",
			cfg:  &ConsumerConfig{Metadata: nil},
			prev: &ConsumerConfig{Metadata: metadataPrevious()},
		},
		{
			desc: "copy-previous",
			cfg:  &ConsumerConfig{Metadata: metadataAllSet("-1")},
			prev: &ConsumerConfig{Metadata: metadataPrevious()},
		},
		{
			desc: "delete-missing-fields",
			cfg:  &ConsumerConfig{Metadata: metadataAllSet("-1")},
			prev: &ConsumerConfig{Metadata: make(map[string]string)},
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			copyConsumerMetadata(test.cfg, test.prev)

			var expectedMetadata map[string]string
			if test.prev != nil {
				expectedMetadata = test.prev.Metadata
			}

			value, ok := expectedMetadata[JSCreatedVersionMetadataKey]
			if ok {
				require_Equal(t, test.cfg.Metadata[JSCreatedVersionMetadataKey], value)
			} else {
				// Key shouldn't exist.
				_, ok = test.cfg.Metadata[JSCreatedVersionMetadataKey]
				require_False(t, ok)
			}

			value, ok = expectedMetadata[JSCreatedLevelMetadataKey]
			if ok {
				require_Equal(t, test.cfg.Metadata[JSCreatedLevelMetadataKey], value)
			} else {
				// Key shouldn't exist.
				_, ok = test.cfg.Metadata[JSCreatedLevelMetadataKey]
				require_False(t, ok)
			}

			value, ok = expectedMetadata[JSRequiredLevelMetadataKey]
			if ok {
				require_Equal(t, test.cfg.Metadata[JSRequiredLevelMetadataKey], value)
			} else {
				// Key shouldn't exist.
				_, ok = test.cfg.Metadata[JSRequiredLevelMetadataKey]
				require_False(t, ok)
			}
		})
	}
}

func TestJetStreamCopyConsumerMetadataRemoveDynamicFields(t *testing.T) {
	dynamicMetadata := func() map[string]string {
		return map[string]string{
			JSServerVersionMetadataKey: "dynamic-version",
			JSServerLevelMetadataKey:   "dynamic-version",
		}
	}

	cfg := ConsumerConfig{Metadata: dynamicMetadata()}
	copyConsumerMetadata(&cfg, nil)
	require_Equal(t, len(cfg.Metadata), 0)

	cfg = ConsumerConfig{Metadata: dynamicMetadata()}
	prevCfg := ConsumerConfig{Metadata: metadataAllSet("0")}
	copyConsumerMetadata(&cfg, &prevCfg)
	require_True(t, reflect.DeepEqual(cfg.Metadata, metadataAllSet("0")))
}

type server struct {
	replicas int
	js       nats.JetStreamContext
	nc       *nats.Conn
}

const (
	streamName   = "STREAM"
	consumerName = "CONSUMER"
)

func TestJetStreamMetadataMutations(t *testing.T) {
	single := RunBasicJetStreamServer(t)
	defer single.Shutdown()
	nc, js := jsClientConnect(t, single)
	defer nc.Close()

	cluster := createJetStreamClusterExplicit(t, "R3S", 3)
	defer cluster.shutdown()
	cnc, cjs := jsClientConnect(t, cluster.randomServer())
	defer cnc.Close()

	// Test for both single server and clustered mode.
	for _, s := range []server{
		{1, js, nc},
		{3, cjs, cnc},
	} {
		t.Run(fmt.Sprintf("R%d", s.replicas), func(t *testing.T) {
			streamMetadataChecks(t, s)
			consumerMetadataChecks(t, s)
		})
	}
}

func validateMetadata(metadata map[string]string, expectedFeatureLevel string) bool {
	return metadata[JSCreatedVersionMetadataKey] == VERSION ||
		metadata[JSCreatedLevelMetadataKey] == strconv.Itoa(JSApiLevel) ||
		metadata[JSRequiredLevelMetadataKey] == expectedFeatureLevel ||
		metadata[JSServerVersionMetadataKey] == VERSION ||
		metadata[JSServerLevelMetadataKey] == strconv.Itoa(JSApiLevel)
}

func streamMetadataChecks(t *testing.T, s server) {
	// Add stream.
	sc := nats.StreamConfig{Name: streamName, Replicas: s.replicas}
	si, err := s.js.AddStream(&sc)
	require_NoError(t, err)
	require_True(t, validateMetadata(si.Config.Metadata, "0"))

	// (Double) add stream, has different code path for clustered streams.
	sc = nats.StreamConfig{Name: streamName, Replicas: s.replicas}
	si, err = s.js.AddStream(&sc)
	require_NoError(t, err)
	require_True(t, validateMetadata(si.Config.Metadata, "0"))

	// Stream info.
	si, err = s.js.StreamInfo(streamName)
	require_NoError(t, err)
	require_True(t, validateMetadata(si.Config.Metadata, "0"))

	// Update stream.
	// Metadata set on creation should be preserved, even if not included in update.
	si, err = s.js.UpdateStream(&sc)
	require_NoError(t, err)
	require_True(t, validateMetadata(si.Config.Metadata, "0"))
}

func consumerMetadataChecks(t *testing.T, s server) {
	// Add consumer.
	cc := nats.ConsumerConfig{Name: consumerName, Replicas: s.replicas}
	ci, err := s.js.AddConsumer(streamName, &cc)
	require_NoError(t, err)
	require_True(t, validateMetadata(ci.Config.Metadata, "0"))

	// Consumer info.
	ci, err = s.js.ConsumerInfo(streamName, consumerName)
	require_NoError(t, err)
	require_True(t, validateMetadata(ci.Config.Metadata, "0"))

	// Update consumer.
	// Metadata set on creation should be preserved, even if not included in update.
	ci, err = s.js.UpdateConsumer(streamName, &cc)
	require_NoError(t, err)
	require_True(t, validateMetadata(ci.Config.Metadata, "0"))

	// Use pause advisories to know when pause/resume is applied.
	pauseCh := make(chan *nats.Msg, 10)
	_, err = s.nc.ChanSubscribe(JSAdvisoryConsumerPausePre+".STREAM.CONSUMER", pauseCh)
	require_NoError(t, err)

	// Pause consumer, should up required API level.
	jsTestPause_PauseConsumer(t, s.nc, streamName, consumerName, time.Now().Add(time.Second*3))
	require_ChanRead(t, pauseCh, time.Second*2)
	require_Len(t, len(pauseCh), 0)

	ci, err = s.js.ConsumerInfo(streamName, consumerName)
	require_NoError(t, err)
	require_True(t, validateMetadata(ci.Config.Metadata, "1"))

	// Unpause consumer, should lower required API level.
	subj := fmt.Sprintf("$JS.API.CONSUMER.PAUSE.%s.%s", streamName, consumerName)
	_, err = s.nc.Request(subj, nil, time.Second)
	require_NoError(t, err)
	require_ChanRead(t, pauseCh, time.Second*2)
	require_Len(t, len(pauseCh), 0)

	ci, err = s.js.ConsumerInfo(streamName, consumerName)
	require_NoError(t, err)
	require_True(t, validateMetadata(ci.Config.Metadata, "0"))

	// Test scaling up/down.
	if s.replicas == 3 {
		// Scale down.
		cc.Replicas = 1
		ci, err = s.js.UpdateConsumer(streamName, &cc)
		require_NoError(t, err)
		require_True(t, validateMetadata(ci.Config.Metadata, "0"))

		ci, err = s.js.ConsumerInfo(streamName, consumerName)
		require_NoError(t, err)
		require_True(t, validateMetadata(ci.Config.Metadata, "0"))

		// Scale up.
		cc.Replicas = 3
		ci, err = s.js.UpdateConsumer(streamName, &cc)
		require_NoError(t, err)
		require_True(t, validateMetadata(ci.Config.Metadata, "0"))

		ci, err = s.js.ConsumerInfo(streamName, consumerName)
		require_NoError(t, err)
		require_True(t, validateMetadata(ci.Config.Metadata, "0"))
	}
}

func TestJetStreamMetadataStreamRestoreAndRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	restoreEmptyStream(t, nc, 1)

	expectedMetadata := map[string]string{
		JSServerVersionMetadataKey: VERSION,
		JSServerLevelMetadataKey:   strconv.Itoa(JSApiLevel),
	}

	// Stream restore should result in empty metadata to be preserved, only adding dynamic metadata.
	si, err := js.StreamInfo(streamName)
	require_NoError(t, err)
	require_True(t, reflect.DeepEqual(si.Config.Metadata, expectedMetadata))

	// Restart server.
	port := s.opts.Port
	sd := s.StoreDir()
	nc.Close()
	s.Shutdown()
	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// After restart (or upgrade) metadata data should remain empty, only adding dynamic metadata.
	si, err = js.StreamInfo(streamName)
	require_NoError(t, err)
	require_True(t, reflect.DeepEqual(si.Config.Metadata, expectedMetadata))
}

func TestJetStreamMetadataStreamRestoreAndRestartCluster(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	restoreEmptyStream(t, nc, 3)

	expectedMetadata := map[string]string{
		JSServerVersionMetadataKey: VERSION,
		JSServerLevelMetadataKey:   strconv.Itoa(JSApiLevel),
	}

	// Stream restore should result in empty metadata to be preserved, only adding dynamic metadata.
	si, err := js.StreamInfo(streamName)
	require_NoError(t, err)
	require_True(t, reflect.DeepEqual(si.Config.Metadata, expectedMetadata))

	// Restart cluster.
	c.stopAll()
	c.restartAllSamePorts()
	defer c.shutdown()
	c.waitOnAllCurrent()
	c.waitOnStreamLeader("$G", streamName)
	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// After restart (or upgrade) metadata data should remain empty, only adding dynamic metadata.
	si, err = js.StreamInfo(streamName)
	require_NoError(t, err)
	require_True(t, reflect.DeepEqual(si.Config.Metadata, expectedMetadata))
}

func restoreEmptyStream(t *testing.T, nc *nats.Conn, replicas int) {
	rreq := JSApiStreamRestoreRequest{
		Config: StreamConfig{
			Name:      "STREAM",
			Retention: LimitsPolicy,
			Storage:   FileStorage,
			Replicas:  replicas,
		},
	}
	buf, err := json.Marshal(rreq)
	require_NoError(t, err)

	var rresp JSApiStreamRestoreResponse
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamRestoreT, rreq.Config.Name), buf, 5*time.Second)
	require_NoError(t, err)
	json.Unmarshal(msg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Error on restore: %+v", rresp.Error)
	}

	// Construct empty stream.tar.s2 (only containing meta.inf).
	fsi := FileStreamInfo{StreamConfig: rreq.Config}
	fsij, err := json.Marshal(fsi)
	require_NoError(t, err)

	hdr := &tar.Header{
		Name:   JetStreamMetaFile,
		Mode:   0600,
		Uname:  "nats",
		Gname:  "nats",
		Size:   int64(len(fsij)),
		Format: tar.FormatPAX,
	}
	var buffer bytes.Buffer
	enc := s2.NewWriter(&buffer)
	tw := tar.NewWriter(enc)
	err = tw.WriteHeader(hdr)
	require_NoError(t, err)
	_, err = tw.Write(fsij)
	require_NoError(t, err)
	err = tw.Close()
	require_NoError(t, err)
	err = enc.Close()
	require_NoError(t, err)

	data := buffer.Bytes()
	msg, err = nc.Request(rresp.DeliverSubject, data, 5*time.Second)
	require_NoError(t, err)
	json.Unmarshal(msg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Error on restore: %+v", rresp.Error)
	}

	msg, err = nc.Request(rresp.DeliverSubject, nil, 5*time.Second)
	require_NoError(t, err)

	expectedMetadata := map[string]string{
		JSServerVersionMetadataKey: VERSION,
		JSServerLevelMetadataKey:   strconv.Itoa(JSApiLevel),
	}

	var cresp JSApiStreamCreateResponse
	err = json.Unmarshal(msg.Data, &cresp)
	require_NoError(t, err)
	require_True(t, reflect.DeepEqual(cresp.Config.Metadata, expectedMetadata))
}
