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

import (
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"io"
	"os"
	"testing"
	"time"
)

func metadataAllSet(featureLevel string) map[string]string {
	return map[string]string{
		JSCreatedVersionMetadataKey: VERSION,
		JSCreatedLevelMetadataKey:   JSApiLevel,
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

func metadataEmpty() map[string]string {
	return map[string]string{
		JSRequiredLevelMetadataKey: "0",
	}
}

func TestJetStreamSetStreamAssetVersionMetadata(t *testing.T) {
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
			expectedMetadata: metadataEmpty(),
		},
		{
			desc:             "update/empty-prev-metadata/delete-user-provided",
			cfg:              &StreamConfig{Metadata: metadataPrevious()},
			prev:             &StreamConfig{},
			expectedMetadata: metadataEmpty(),
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			setStreamAssetVersionMetadata(test.cfg, test.prev)
			require_Equal(t, test.cfg.Metadata[JSCreatedVersionMetadataKey], test.expectedMetadata[JSCreatedVersionMetadataKey])
			require_Equal(t, test.cfg.Metadata[JSCreatedLevelMetadataKey], test.expectedMetadata[JSCreatedLevelMetadataKey])
			require_Equal(t, test.cfg.Metadata[JSRequiredLevelMetadataKey], test.expectedMetadata[JSRequiredLevelMetadataKey])
		})
	}
}

func TestJetStreamSetConsumerAssetVersionMetadata(t *testing.T) {
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
			desc:             "update/empty-prev-metadata",
			cfg:              &ConsumerConfig{},
			prev:             &ConsumerConfig{},
			expectedMetadata: metadataEmpty(),
		},
		{
			desc:             "update/empty-prev-metadata/delete-user-provided",
			cfg:              &ConsumerConfig{Metadata: metadataPrevious()},
			prev:             &ConsumerConfig{},
			expectedMetadata: metadataEmpty(),
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			setConsumerAssetVersionMetadata(test.cfg, test.prev)
			require_Equal(t, test.cfg.Metadata[JSCreatedVersionMetadataKey], test.expectedMetadata[JSCreatedVersionMetadataKey])
			require_Equal(t, test.cfg.Metadata[JSCreatedLevelMetadataKey], test.expectedMetadata[JSCreatedLevelMetadataKey])
			require_Equal(t, test.cfg.Metadata[JSRequiredLevelMetadataKey], test.expectedMetadata[JSRequiredLevelMetadataKey])
		})
	}
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

func TestJetStreamAssetVersionMetadataMutations(t *testing.T) {
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
			streamAssetVersionChecks(t, s)
			consumerAssetVersionChecks(t, s)
		})
	}
}

func streamAssetVersionChecks(t *testing.T, s server) {
	// Add stream.
	sc := nats.StreamConfig{Name: streamName, Replicas: s.replicas}
	si, err := s.js.AddStream(&sc)
	require_NoError(t, err)
	require_Equal(t, si.Config.Metadata[JSCreatedVersionMetadataKey], VERSION)
	require_Equal(t, si.Config.Metadata[JSCreatedLevelMetadataKey], JSApiLevel)
	require_Equal(t, si.Config.Metadata[JSRequiredLevelMetadataKey], "0")

	// Stream info.
	si, err = s.js.StreamInfo(streamName)
	require_NoError(t, err)
	require_Equal(t, si.Config.Metadata[JSCreatedVersionMetadataKey], VERSION)
	require_Equal(t, si.Config.Metadata[JSCreatedLevelMetadataKey], JSApiLevel)
	require_Equal(t, si.Config.Metadata[JSRequiredLevelMetadataKey], "0")

	// Update stream.
	// Metadata set on creation should be preserved, even if not included in update.
	si, err = s.js.UpdateStream(&sc)
	require_NoError(t, err)
	require_Equal(t, si.Config.Metadata[JSCreatedVersionMetadataKey], VERSION)
	require_Equal(t, si.Config.Metadata[JSCreatedLevelMetadataKey], JSApiLevel)
	require_Equal(t, si.Config.Metadata[JSRequiredLevelMetadataKey], "0")
}

func consumerAssetVersionChecks(t *testing.T, s server) {
	// Add consumer.
	cc := nats.ConsumerConfig{Name: consumerName, Replicas: s.replicas}
	ci, err := s.js.AddConsumer(streamName, &cc)
	require_NoError(t, err)
	require_Equal(t, ci.Config.Metadata[JSCreatedVersionMetadataKey], VERSION)
	require_Equal(t, ci.Config.Metadata[JSCreatedLevelMetadataKey], JSApiLevel)
	require_Equal(t, ci.Config.Metadata[JSRequiredLevelMetadataKey], "0")

	// Consumer info.
	ci, err = s.js.ConsumerInfo(streamName, consumerName)
	require_NoError(t, err)
	require_Equal(t, ci.Config.Metadata[JSCreatedVersionMetadataKey], VERSION)
	require_Equal(t, ci.Config.Metadata[JSCreatedLevelMetadataKey], JSApiLevel)
	require_Equal(t, ci.Config.Metadata[JSRequiredLevelMetadataKey], "0")

	// Update consumer.
	// Metadata set on creation should be preserved, even if not included in update.
	ci, err = s.js.UpdateConsumer(streamName, &cc)
	require_NoError(t, err)
	require_Equal(t, ci.Config.Metadata[JSCreatedVersionMetadataKey], VERSION)
	require_Equal(t, ci.Config.Metadata[JSCreatedLevelMetadataKey], JSApiLevel)
	require_Equal(t, ci.Config.Metadata[JSRequiredLevelMetadataKey], "0")

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
	require_Equal(t, ci.Config.Metadata[JSCreatedVersionMetadataKey], VERSION)
	require_Equal(t, ci.Config.Metadata[JSCreatedLevelMetadataKey], JSApiLevel)
	require_Equal(t, ci.Config.Metadata[JSRequiredLevelMetadataKey], "1")

	// Unpause consumer, should lower required API level.
	subj := fmt.Sprintf("$JS.API.CONSUMER.PAUSE.%s.%s", streamName, consumerName)
	_, err = s.nc.Request(subj, nil, time.Second)
	require_NoError(t, err)
	require_ChanRead(t, pauseCh, time.Second*2)
	require_Len(t, len(pauseCh), 0)

	ci, err = s.js.ConsumerInfo(streamName, consumerName)
	require_NoError(t, err)
	require_Equal(t, ci.Config.Metadata[JSCreatedVersionMetadataKey], VERSION)
	require_Equal(t, ci.Config.Metadata[JSCreatedLevelMetadataKey], JSApiLevel)
	require_Equal(t, ci.Config.Metadata[JSRequiredLevelMetadataKey], "0")
}

func TestJetStreamAssetVersionMetadataStreamRestoreAndRestart(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	path := "../test/configs/jetstream/restore_empty_R1F_stream"
	restoreStreamFromPath(t, nc, path)

	// Stream restore should result in empty metadata to be preserved.
	si, err := js.StreamInfo(streamName)
	require_NoError(t, err)
	require_Equal(t, len(si.Config.Metadata), 0)

	// Restart server.
	port := s.opts.Port
	sd := s.StoreDir()
	nc.Close()
	s.Shutdown()
	s = RunJetStreamServerOnPort(port, sd)
	defer s.Shutdown()
	nc, js = jsClientConnect(t, s)
	defer nc.Close()

	// After restart (or upgrade) metadata data should remain empty.
	si, err = js.StreamInfo(streamName)
	require_NoError(t, err)
	require_Equal(t, len(si.Config.Metadata), 0)
}

func TestJetStreamAssetVersionMetadataStreamRestoreAndRestartCluster(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()
	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	path := "../test/configs/jetstream/restore_empty_R3F_stream"
	restoreStreamFromPath(t, nc, path)

	// Stream restore should result in empty metadata to be preserved.
	si, err := js.StreamInfo(streamName)
	require_NoError(t, err)
	require_Equal(t, len(si.Config.Metadata), 0)

	// Restart cluster.
	c.stopAll()
	c.restartAllSamePorts()
	defer c.shutdown()
	c.waitOnAllCurrent()
	c.waitOnStreamLeader("$G", streamName)
	nc, js = jsClientConnect(t, c.randomServer())
	defer nc.Close()

	// After restart (or upgrade) metadata data should remain empty.
	si, err = js.StreamInfo(streamName)
	require_NoError(t, err)
	require_Equal(t, len(si.Config.Metadata), 0)
}

func restoreStreamFromPath(t *testing.T, nc *nats.Conn, path string) {
	var rreq JSApiStreamRestoreRequest
	buf, err := os.ReadFile(fmt.Sprintf("%s/backup.json", path))
	require_NoError(t, err)
	err = json.Unmarshal(buf, &rreq)
	require_NoError(t, err)

	data, err := os.Open(fmt.Sprintf("%s/stream.tar.s2", path))
	require_NoError(t, err)
	defer data.Close()

	var rresp JSApiStreamRestoreResponse
	msg, err := nc.Request(fmt.Sprintf(JSApiStreamRestoreT, rreq.Config.Name), buf, 5*time.Second)
	require_NoError(t, err)
	json.Unmarshal(msg.Data, &rresp)
	if rresp.Error != nil {
		t.Fatalf("Error on restore: %+v", rresp.Error)
	}

	var chunk [1024]byte
	for {
		n, err := data.Read(chunk[:])
		if err == io.EOF {
			break
		}
		require_NoError(t, err)

		msg, err = nc.Request(rresp.DeliverSubject, chunk[:n], 5*time.Second)
		require_NoError(t, err)
		json.Unmarshal(msg.Data, &rresp)
		if rresp.Error != nil {
			t.Fatalf("Error on restore: %+v", rresp.Error)
		}
	}
	msg, err = nc.Request(rresp.DeliverSubject, nil, 5*time.Second)
	require_NoError(t, err)
	err = json.Unmarshal(msg.Data, &rresp)
	require_NoError(t, err)
}
