// Copyright 2021-2021 The NATS Authors
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
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func getOkTraceBody(t *testing.T, msg *nats.Msg) (*ServerInfo, *TraceMsg) {
	t.Helper()
	tMsg := ServerAPIResponse{Data: &TraceMsg{}}
	require_NoError(t, json.Unmarshal(msg.Data, &tMsg))
	require_True(t, tMsg.Error == nil)
	// basic server info sanity check
	require_True(t, tMsg.Server.Name != _EMPTY_)
	require_True(t, tMsg.Server.ID != _EMPTY_)
	require_True(t, tMsg.Server.Version != _EMPTY_)
	require_True(t, tMsg.Server.Seq != 0)
	require_True(t, tMsg.Server.Time.Before(time.Now()))

	//TODO uncomment to see server trace messages
	//LogServerApiResponse(t, msg)

	return tMsg.Server, tMsg.Data.(*TraceMsg)
}

func LogServerApiResponse(t *testing.T, msg *nats.Msg) {
	t.Helper()
	tMsg := ServerAPIResponse{}
	data := msg.Data
	err := json.Unmarshal(data, &tMsg)
	require_NoError(t, err)
	data, err = json.MarshalIndent(&tMsg, "", "  ")
	require_NoError(t, err)
	t.Logf("ServerAPIResponse %s:\n%s\n", msg.Subject, string(data))
}

func requie_BasicSubNatsClient(t *testing.T, tSub *TraceSubscription, acc string, subscription, msgSubject, queue, importTo string) {
	t.Helper()
	require_Equal(t, tSub.Acc, acc)
	require_Equal(t, tSub.SubClient.Kind, "Client")
	require_Equal(t, tSub.SubClient.Type, "nats")
	require_True(t, tSub.SubClient.ClientId != 0)
	require_Equal(t, tSub.Sub, subscription)
	require_Equal(t, tSub.PubSubject, msgSubject)
	require_Equal(t, tSub.Queue, queue)
	require_Equal(t, tSub.ImportTo, importTo)
}

func requie_BasicPubNatsClient(t *testing.T, tMsg *TraceMsg, acc string) {
	t.Helper()
	require_Equal(t, tMsg.Acc, acc)
	require_Equal(t, tMsg.Client.Kind, "Client")
	require_Equal(t, tMsg.Client.Type, "nats")
	require_True(t, tMsg.Client.ClientId != 0)
}

func TestMessageTraceOperatorSimpleServer(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	confA := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		system_account: SYS
		server_name: srv-A
		jetstream: {max_mem_store: 10Mb, max_file_store: 10Mb, store_dir: %s }
		operator_msg_trace: {
			trace_particular_subject_always: {Subject: always, Probes: [connz, accountz, varz] }
		}
		accounts: {
			SYS: {users: [ {user: sys, password: sys}]}
			ACC: {users: [ {user: acc, password: acc}]}
			MAP: {
				users: [ {user: map, password: map}]
				mappings: {map.*: "$1"}
			}
			EXP_STRM: {
				users: [ {user: exp_strm, password: exp_strm}]
				exports: [{stream: >}]
			}
			IMP_STRM: {
				users: [ {user: imp_strm, password: imp_strm}]
				imports: [{stream: {account: EXP_STRM, subject: *}, to: imported.$1}]
			}
			EXP_SVC: {
				users: [ {user: exp_svc, password: exp_svc}]
				exports: [{service: svc.>}]
			}
			IMP_SVC: {
				users: [ {user: imp_svc, password: imp_svc}]
				imports: [{service: {account: EXP_SVC, subject: svc.*}, to: *}]
			}

			SVC_CHAIN1: {
				users: [ {user: svc_chain1, password: svc_chain1}]
				exports: [{service: svc1.>}]
			}
			SVC_CHAIN2: {
				users: [ {user: svc_chain2, password: svc_chain2}]
				imports: [{service: {account: SVC_CHAIN1, subject: svc1.*}, to: svc2.*}]
				exports: [{service: svc2.>}]
			}
			SVC_CHAIN3: {
				users: [ {user: svc_chain3, password: svc_chain3}]
				imports: [{service: {account: SVC_CHAIN2, subject: svc2.*}, to: *}]
			}

			JS_ACC {
				users: [ {user: js_acc, password: js_acc}]
				jetstream: {max_mem: 5Mb, max_store: 5Mb, max_streams: -1, max_consumers: -1}
			}
		}
    `, storeDir)))
	defer removeFile(t, confA)
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()

	ncSys := natsConnect(t, sA.ClientURL(), nats.UserInfo("sys", "sys"))
	defer ncSys.Close()

	s, err := ncSys.SubscribeSync(fmt.Sprintf(serverTrcEvent, "*", "trace_particular_subject_always"))
	require_NoError(t, err)
	require_NoError(t, ncSys.Flush())

	createAccountConnPair := func(accUser1, accUser2 string) (*nats.Conn, *nats.Conn) {
		t.Helper()
		ncPub := natsConnect(t, sA.ClientURL(), nats.UserInfo(accUser1, accUser1))
		require_NoError(t, ncPub.Flush())
		ncSub := natsConnect(t, sA.ClientURL(), nats.UserInfo(accUser2, accUser2))
		require_NoError(t, ncSub.Flush())
		return ncPub, ncSub
	}
	require_TimeoutOnPolicy := func() {
		t.Helper()
		_, err := s.NextMsg(250 * time.Millisecond)
		require_True(t, err == nats.ErrTimeout)
	}
	sendTestData := func(nc *nats.Conn, subjects ...string) {
		t.Helper()
		for _, subj := range subjects {
			require_NoError(t, nc.Publish(subj, nil))
		}
		require_NoError(t, nc.Flush())
	}
	recvTestData := func(sub *nats.Subscription, num int) {
		t.Helper()
		for i := 0; i < num; i++ {
			_, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
		}
	}

	t.Run("no-sub", func(t *testing.T) {
		ncPub, ncSub := createAccountConnPair("acc", "acc")
		defer ncPub.Close()
		defer ncSub.Close()
		sendTestData(ncPub, "never", "always")

		msg, err := s.NextMsg(time.Second)
		require_NoError(t, err)

		_, tMsg := getOkTraceBody(t, msg)

		require_True(t, len(tMsg.Subscriptions) == 0)
		requie_BasicPubNatsClient(t, tMsg, "ACC")
		require_Equal(t, tMsg.SubjectMatched, "always")
		require_True(t, len(tMsg.AssociatedInfo.AccountInfo) == 1)
		require_True(t, len(tMsg.AssociatedInfo.ConnInfo) == 1)
		require_TimeoutOnPolicy()
	})
	// next two tests are almost identical, only differ if same or different connection
	testWithSub := func(ncPub, ncSub *nats.Conn) *TraceMsg {
		sub, err := ncSub.SubscribeSync(">")
		require_NoError(t, err)
		require_NoError(t, ncSub.Flush())

		sendTestData(ncPub, "never", "always")
		recvTestData(sub, 2)

		msg, err := s.NextMsg(time.Second)
		require_NoError(t, err)

		_, tMsg := getOkTraceBody(t, msg)

		require_True(t, len(tMsg.Subscriptions) == 1)
		requie_BasicSubNatsClient(t, tMsg.Subscriptions[0], "ACC", ">", "always", "", "")

		requie_BasicPubNatsClient(t, tMsg, "ACC")
		require_Equal(t, tMsg.SubjectMatched, "always")

		require_TimeoutOnPolicy()
		return tMsg
	}
	t.Run("with-sub-different-con", func(t *testing.T) {
		ncPub, ncSub := createAccountConnPair("acc", "acc")
		defer ncPub.Close()
		defer ncSub.Close()
		tMsg := testWithSub(ncPub, ncSub)
		require_True(t, tMsg.Client.ClientId != tMsg.Subscriptions[0].SubClient.ClientId)
		require_True(t, len(tMsg.AssociatedInfo.AccountInfo) == 1)
		require_True(t, len(tMsg.AssociatedInfo.ConnInfo) == 2)
	})
	t.Run("with-sub-same-Con", func(t *testing.T) {
		ncPub, ncSub := createAccountConnPair("acc", "acc")
		ncPub.Close()
		defer ncSub.Close()
		tMsg := testWithSub(ncSub, ncSub)
		require_True(t, tMsg.Client.ClientId == tMsg.Subscriptions[0].SubClient.ClientId)
		require_True(t, len(tMsg.AssociatedInfo.AccountInfo) == 1)
		require_True(t, len(tMsg.AssociatedInfo.ConnInfo) == 1)
	})
	t.Run("no-echo", func(t *testing.T) {
		nc := natsConnect(t, sA.ClientURL(), nats.UserInfo("acc", "acc"), nats.NoEcho())
		defer nc.Close()
		sub, err := nc.SubscribeSync(">")
		require_NoError(t, err)
		require_NoError(t, nc.Flush())
		err = nc.Publish("always", nil)
		require_NoError(t, err)
		require_NoError(t, nc.Flush())
		_, err = sub.NextMsg(time.Second / 4)
		require_True(t, err == nats.ErrTimeout)
		msg, err := s.NextMsg(time.Second)
		require_NoError(t, err)
		_, tMsg := getOkTraceBody(t, msg)
		require_True(t, len(tMsg.Subscriptions) == 1)
		require_Equal(t, tMsg.Subscriptions[0].NoSendReason, "client disallows echo")

	})
	t.Run("with-queue-sub", func(t *testing.T) {
		ncPub, ncSub := createAccountConnPair("acc", "acc")
		defer ncPub.Close()
		defer ncSub.Close()
		ch := make(chan *nats.Msg, 3)
		defer close(ch)
		sub1, err := ncSub.ChanQueueSubscribe(">", "queue", ch)
		require_NoError(t, err)
		defer sub1.Unsubscribe()
		sub2, err := ncSub.ChanQueueSubscribe(">", "queue", ch)
		require_NoError(t, err)
		defer sub2.Unsubscribe()
		require_NoError(t, ncSub.Flush())

		sendTestData(ncPub, "never", "always")
		cnt := 0
	BREAK_FOR:
		for {
			select {
			case <-ch:
				cnt++
			case <-time.After(time.Second / 4):
				break BREAK_FOR
			}
		}
		require_True(t, cnt == 2)

		msg, err := s.NextMsg(time.Second)
		require_NoError(t, err)

		_, tMsg := getOkTraceBody(t, msg)

		require_True(t, len(tMsg.Subscriptions) == 1)
		requie_BasicSubNatsClient(t, tMsg.Subscriptions[0], "ACC", ">", "always", "queue", "")

		requie_BasicPubNatsClient(t, tMsg, "ACC")
		require_Equal(t, tMsg.SubjectMatched, "always")

		require_TimeoutOnPolicy()
	})
	t.Run("mapped-sub", func(t *testing.T) {
		ncPub, ncSub := createAccountConnPair("map", "map")
		defer ncPub.Close()
		defer ncSub.Close()

		sub, err := ncSub.SubscribeSync(">")
		require_NoError(t, err)
		require_NoError(t, ncSub.Flush())

		sendTestData(ncPub, "map.always")
		recvTestData(sub, 1)

		msg, err := s.NextMsg(time.Second)
		require_NoError(t, err)

		_, tMsg := getOkTraceBody(t, msg)

		require_True(t, len(tMsg.Subscriptions) == 1)
		requie_BasicSubNatsClient(t, tMsg.Subscriptions[0], "MAP", ">", "always", "", "")

		requie_BasicPubNatsClient(t, tMsg, "MAP")
		require_Equal(t, tMsg.SubjectMatched, "always")
		require_Equal(t, tMsg.SubjectPreMapping, "map.always")
		require_True(t, len(tMsg.AssociatedInfo.AccountInfo) == 1)
		require_True(t, len(tMsg.AssociatedInfo.ConnInfo) == 2)
		require_TimeoutOnPolicy()
	})
	t.Run("js-sub", func(t *testing.T) {
		ncPub, ncSub := createAccountConnPair("js_acc", "js_acc")
		defer ncPub.Close()
		defer ncSub.Close()

		js, err := ncSub.JetStream()
		require_NoError(t, err)

		_, err = js.AddStream(&nats.StreamConfig{Name: "S1", Subjects: []string{"always"}})
		require_NoError(t, err)
		defer js.DeleteStream("S1")

		sendTestData(ncPub, "always")

		msg, err := s.NextMsg(time.Second)
		require_NoError(t, err)

		_, tMsg := getOkTraceBody(t, msg)

		require_True(t, len(tMsg.Subscriptions) == 1)
		require_Equal(t, tMsg.Subscriptions[0].Acc, "JS_ACC")
		require_Equal(t, tMsg.Subscriptions[0].PubSubject, "always")
		require_True(t, tMsg.Subscriptions[0].SubClient != nil)
		require_Equal(t, tMsg.Subscriptions[0].SubClient.Kind, "JetStream")

		require_True(t, len(tMsg.Stores) == 1)
		require_Equal(t, tMsg.Stores[0].Acc, "JS_ACC")
		require_Equal(t, tMsg.Stores[0].PubSubject, "always")
		require_Equal(t, tMsg.Stores[0].StreamRef, "S1")
		require_True(t, tMsg.Stores[0].StreamType == "File")
		require_True(t, tMsg.Stores[0].StreamRepl == 1)
		require_True(t, tMsg.Stores[0].DidStore)
		require_Equal(t, tMsg.Subscriptions[0].Ref, tMsg.Stores[0].MsgRef)
		require_TimeoutOnPolicy()

		jsSub, err := js.SubscribeSync("always", nats.ManualAck())
		require_NoError(t, err)
		require_NoError(t, ncSub.Flush())
		m, err := jsSub.NextMsg(time.Second)
		require_NoError(t, err)
		msg, err = s.NextMsg(time.Second)
		require_NoError(t, err)
		getOkTraceBody(t, msg)
		// make sure there are no additional trace messages
		require_TimeoutOnPolicy()

		// Nack so we can retrieve the message again
		require_NoError(t, m.Nak())
		_, err = jsSub.NextMsg(time.Second)
		require_NoError(t, err)
		msg, err = s.NextMsg(time.Second)
		require_NoError(t, err)
		getOkTraceBody(t, msg)

		require_TimeoutOnPolicy()
	})
	t.Run("import-stream", func(t *testing.T) {
		ncPub, ncSub := createAccountConnPair("exp_strm", "imp_strm")
		defer ncPub.Close()
		defer ncSub.Close()

		sub1, err := ncSub.SubscribeSync(">")
		require_NoError(t, err)
		require_NoError(t, ncSub.Flush())

		sendTestData(ncPub, "always")
		recvTestData(sub1, 1)

		msg, err := s.NextMsg(time.Second)
		require_NoError(t, err)

		_, tMsg := getOkTraceBody(t, msg)

		require_True(t, len(tMsg.Subscriptions) == 1)
		requie_BasicSubNatsClient(t, tMsg.Subscriptions[0], "IMP_STRM", "*", "imported.always", "", "imported.$1")
		requie_BasicPubNatsClient(t, tMsg, "EXP_STRM")
		require_Equal(t, tMsg.SubjectMatched, "always")

		require_True(t, len(tMsg.AssociatedInfo.AccountInfo) == 2)
		require_True(t, len(tMsg.AssociatedInfo.ConnInfo) == 2)
		require_TimeoutOnPolicy()
	})
	t.Run("import-service", func(t *testing.T) {
		ncPub, ncSub := createAccountConnPair("imp_svc", "exp_svc")
		defer ncPub.Close()
		defer ncSub.Close()

		sub, err := ncSub.SubscribeSync(">")
		require_NoError(t, err)
		require_NoError(t, ncSub.Flush())

		sendTestData(ncPub, "always")
		recvTestData(sub, 1)

		msg, err := s.NextMsg(time.Second)
		require_NoError(t, err)

		_, tMsg := getOkTraceBody(t, msg)

		require_True(t, len(tMsg.Subscriptions) == 1)
		requie_BasicSubNatsClient(t, tMsg.Subscriptions[0], "EXP_SVC", ">", "svc.always", "", "svc.$1")
		requie_BasicPubNatsClient(t, tMsg, "IMP_SVC")
		require_Equal(t, tMsg.SubjectMatched, "always")

		require_True(t, len(tMsg.AssociatedInfo.AccountInfo) == 2)
		require_True(t, len(tMsg.AssociatedInfo.ConnInfo) == 2)
		require_TimeoutOnPolicy()
	})
	t.Run("service-chain-1", func(t *testing.T) {
		ncPub, ncSub := createAccountConnPair("svc_chain3", "svc_chain1")
		defer ncPub.Close()
		defer ncSub.Close()

		sub, err := ncSub.SubscribeSync(">")
		require_NoError(t, err)
		require_NoError(t, ncSub.Flush())

		sendTestData(ncPub, "always")
		recvTestData(sub, 1)

		msg, err := s.NextMsg(time.Second)
		require_NoError(t, err)

		_, tmsg := getOkTraceBody(t, msg)
		require_TimeoutOnPolicy()

		require_Equal(t, tmsg.Acc, "SVC_CHAIN3")
		require_True(t, len(tmsg.Subscriptions) == 1)
		require_Equal(t, tmsg.Subscriptions[0].Acc, "SVC_CHAIN1")
		require_True(t, len(tmsg.AssociatedInfo.AccountInfo) == 2)
		require_True(t, len(tmsg.AssociatedInfo.ConnInfo) == 2)
	})
	t.Run("service-chain-2", func(t *testing.T) {
		// This time have another subscriber in chain2
		ncMid := natsConnect(t, sA.ClientURL(), nats.UserInfo("svc_chain2", "svc_chain2"))
		require_NoError(t, ncMid.Flush())
		sMid, err := ncMid.SubscribeSync(">")
		require_NoError(t, err)

		ncPub, ncSub := createAccountConnPair("svc_chain3", "svc_chain1")
		defer ncPub.Close()
		defer ncSub.Close()

		sub, err := ncSub.SubscribeSync(">")
		require_NoError(t, err)
		require_NoError(t, ncSub.Flush())

		sendTestData(ncPub, "always")
		recvTestData(sMid, 1)
		recvTestData(sub, 1)

		msg, err := s.NextMsg(time.Second)
		require_NoError(t, err)

		_, tmsg := getOkTraceBody(t, msg)
		require_TimeoutOnPolicy()

		require_Equal(t, tmsg.Acc, "SVC_CHAIN3")

		require_True(t, len(tmsg.Subscriptions) == 2)
		accs := map[string]struct{}{}
		for _, sub := range tmsg.Subscriptions {
			accs[sub.Acc] = struct{}{}
			require_Equal(t, sub.SubClient.Kind, "Client")
		}
		_, ok := accs["SVC_CHAIN1"]
		require_True(t, ok)
		_, ok = accs["SVC_CHAIN2"]
		require_True(t, ok)

		require_True(t, len(tmsg.AssociatedInfo.AccountInfo) == 3)
		require_True(t, len(tmsg.AssociatedInfo.ConnInfo) == 3)
	})
}

func TestMessageTraceAccountSimpleServer(t *testing.T) {
	storeDir := createDir(t, JetStreamStoreDir)
	confA := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		system_account: SYS
		server_name: srv-A
		jetstream: {max_mem_store: 10Mb, max_file_store: 10Mb, store_dir: %s }
		accounts: {
			SYS: {users: [ {user: sys, password: sys}]}
			ACC: {
				users: [ {user: acc, password: acc}]
				msg_trace: {
					trace_particular_subject_always: {Subject: always, Probes: [connz]}
					capture_msg: {Subject: capture, Probes: [msg]}
				}
				ping_probes: connz
			}
			MAP: {
				users: [ {user: map, password: map}]
				mappings: {map.*: "$1"}
				msg_trace: {
					trace_particular_subject_always: {Subject: always}
				}
			}
			EXP_STRM: {
				users: [ {user: exp_strm, password: exp_strm}]
				exports: [{stream: >}]
				msg_trace: {
					trace_particular_subject_always: {Subject: always}
				}
			}
			IMP_STRM: {
				users: [ {user: imp_strm, password: imp_strm}]
				imports: [{stream: {account: EXP_STRM, subject: *}, to: imported.$1}]
				msg_trace: {
					trace_particular_subject_always: {Subject: imported.always}
				}
			}
			EXP_SVC: {
				users: [ {user: exp_svc, password: exp_svc}]
				exports: [{service: svc.>}]
				msg_trace: {
					trace_particular_subject_always: {Subject: svc.always}
				}
			}
			IMP_SVC: {
				users: [ {user: imp_svc, password: imp_svc}]
				imports: [{service: {account: EXP_SVC, subject: svc.*}, to: *}]
				msg_trace: {
					trace_particular_subject_always: {Subject: always}
				}
			}
			JS_ACC {
				users: [ {user: js_acc, password: js_acc}]
				jetstream: {max_mem: 5Mb, max_store: 5Mb, max_streams: -1, max_consumers: -1}
				ping_probes: enabled
			}
		}
    `, storeDir)))
	defer removeFile(t, confA)
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()

	createTrcSubExt := func(accUser string, traceSubj string) (*nats.Conn, *nats.Subscription) {
		ncSys := natsConnect(t, sA.ClientURL(), nats.UserInfo(accUser, accUser))
		s, err := ncSys.SubscribeSync(traceSubj)
		require_NoError(t, err)
		require_NoError(t, ncSys.Flush())
		return ncSys, s
	}
	createTrcSub := func(accUser string) (*nats.Conn, *nats.Subscription) {
		return createTrcSubExt(accUser, fmt.Sprintf(accTrcEvent, strings.ToUpper(accUser), "trace_particular_subject_always"))
	}

	createAccountConnPair := func(accUser1, accUser2 string) (*nats.Conn, *nats.Conn) {
		t.Helper()
		ncPub := natsConnect(t, sA.ClientURL(), nats.UserInfo(accUser1, accUser1), nats.Name("ncpub"))
		require_NoError(t, ncPub.Flush())
		ncSub := natsConnect(t, sA.ClientURL(), nats.UserInfo(accUser2, accUser2), nats.Name("ncsub"))
		require_NoError(t, ncSub.Flush())
		return ncPub, ncSub
	}
	require_TimeoutOnPolicy := func(s *nats.Subscription) {
		t.Helper()
		_, err := s.NextMsg(250 * time.Millisecond)
		require_True(t, err == nats.ErrTimeout)
	}
	require_AccountOnlyClient := func(fMsg *TraceMsg, acc string) {
		t.Helper()
		require_Equal(t, fMsg.Acc, acc)
		require_True(t, fMsg.Client == nil)
		require_Equal(t, fMsg.SubjectMatched, "")
		require_Equal(t, fMsg.SubjectPreMapping, "")
	}
	sendTestData := func(nc *nats.Conn, subjects ...string) {
		t.Helper()
		for _, subj := range subjects {
			require_NoError(t, nc.Publish(subj, nil))
		}
		require_NoError(t, nc.Flush())
	}
	recvTestData := func(sub *nats.Subscription, num int) {
		t.Helper()
		for i := 0; i < num; i++ {
			_, err := sub.NextMsg(time.Second)
			require_NoError(t, err)
		}
	}

	t.Run("no-sub", func(t *testing.T) {
		trcCon, s := createTrcSub("acc")
		defer trcCon.Close()
		ncPub, ncSub := createAccountConnPair("acc", "acc")
		defer ncPub.Close()
		defer ncSub.Close()
		sendTestData(ncPub, "never", "always")

		msg, err := s.NextMsg(time.Second)
		require_NoError(t, err)

		_, tMsg := getOkTraceBody(t, msg)

		require_True(t, len(tMsg.Subscriptions) == 0)
		requie_BasicPubNatsClient(t, tMsg, "ACC")
		require_Equal(t, tMsg.SubjectMatched, "always")

		require_TimeoutOnPolicy(s)
	})
	// next two tests are almost identical, only differ if same or different connection
	testWithSub := func(ncPub, ncSub *nats.Conn) *TraceMsg {
		trcCon, s := createTrcSub("acc")
		defer trcCon.Close()
		sub, err := ncSub.SubscribeSync(">")
		require_NoError(t, err)
		require_NoError(t, ncSub.Flush())

		sendTestData(ncPub, "never", "always")
		recvTestData(sub, 2)

		msg, err := s.NextMsg(time.Second)
		require_NoError(t, err)

		_, tMsg := getOkTraceBody(t, msg)

		require_True(t, len(tMsg.Subscriptions) == 1)
		requie_BasicSubNatsClient(t, tMsg.Subscriptions[0], "ACC", ">", "always", "", "")

		requie_BasicPubNatsClient(t, tMsg, "ACC")
		require_Equal(t, tMsg.SubjectMatched, "always")

		require_TimeoutOnPolicy(s)
		return tMsg
	}
	t.Run("with-sub-different-con", func(t *testing.T) {
		ncPub, ncSub := createAccountConnPair("acc", "acc")
		defer ncPub.Close()
		defer ncSub.Close()
		tMsg := testWithSub(ncPub, ncSub)
		require_True(t, tMsg.Client.ClientId != tMsg.Subscriptions[0].SubClient.ClientId)
		require_True(t, len(tMsg.AssociatedInfo.ConnInfo) == 2)
	})
	t.Run("with-sub-same-Con", func(t *testing.T) {
		ncPub, ncSub := createAccountConnPair("acc", "acc")
		ncPub.Close()
		defer ncSub.Close()
		tMsg := testWithSub(ncSub, ncSub)
		require_True(t, tMsg.Client.ClientId == tMsg.Subscriptions[0].SubClient.ClientId)
		require_True(t, len(tMsg.AssociatedInfo.ConnInfo) == 1)
	})
	t.Run("with-queue-sub", func(t *testing.T) {
		trcCon, s := createTrcSub("acc")
		defer trcCon.Close()
		ncPub, ncSub := createAccountConnPair("acc", "acc")
		defer ncPub.Close()
		defer ncSub.Close()
		sub, err := ncSub.QueueSubscribeSync(">", "queue")
		require_NoError(t, err)
		require_NoError(t, ncSub.Flush())

		sendTestData(ncPub, "never", "always")
		recvTestData(sub, 2)

		msg, err := s.NextMsg(time.Second)
		require_NoError(t, err)

		_, tMsg := getOkTraceBody(t, msg)

		require_True(t, len(tMsg.Subscriptions) == 1)
		requie_BasicSubNatsClient(t, tMsg.Subscriptions[0], "ACC", ">", "always", "queue", "")

		requie_BasicPubNatsClient(t, tMsg, "ACC")
		require_Equal(t, tMsg.SubjectMatched, "always")

		require_TimeoutOnPolicy(s)
	})
	t.Run("header-no-sub", func(t *testing.T) {
		ncPub, ncSub := createAccountConnPair("acc", "acc")
		defer ncPub.Close()
		defer ncSub.Close()
		// expecting at least one message, even without subscriber
		msg, err := ncPub.RequestMsg(&nats.Msg{
			Subject: "foo",
			Header: nats.Header{
				TracePing: []string{""},
			}}, time.Second/4)
		require_NoError(t, err)
		_, tMsg := getOkTraceBody(t, msg)
		require_True(t, tMsg.AssociatedInfo == nil)
		require_True(t, len(tMsg.Subscriptions) == 0)
	})
	t.Run("header-trigger", func(t *testing.T) {
		ncPub, ncSub := createAccountConnPair("acc", "acc")
		defer ncPub.Close()
		defer ncSub.Close()
		sub, err := ncSub.SubscribeSync("foo")
		require_NoError(t, err)
		require_NoError(t, ncSub.Flush())
		t.Run("no-request", func(t *testing.T) {
			// expected to trigger a trace
			require_NoError(t, ncPub.PublishMsg(&nats.Msg{
				Subject: "foo",
				Header: nats.Header{
					TracePing: []string{""},
				}}))
			require_NoError(t, err)
		})
		t.Run("no-probes", func(t *testing.T) {
			// expected to trigger a trace
			msg, err := ncPub.RequestMsg(&nats.Msg{
				Subject: "foo",
				Header: nats.Header{
					TracePing: []string{""},
				}}, time.Second/4)
			require_NoError(t, err)
			_, tMsg := getOkTraceBody(t, msg)
			require_True(t, tMsg.AssociatedInfo == nil)
		})
		t.Run("with-probes", func(t *testing.T) {
			// expected to trigger a trace
			msg, err := ncPub.RequestMsg(&nats.Msg{
				Subject: "foo",
				Header: nats.Header{
					TracePing: []string{"connz,accountz"},
				}}, time.Second/4)
			require_NoError(t, err)
			_, tMsg := getOkTraceBody(t, msg)
			require_True(t, len(tMsg.AssociatedInfo.ConnInfo) == 2)
			// permission to grant accountz info was not granted by config
			require_True(t, len(tMsg.AssociatedInfo.AccountInfo) == 0)
			require_True(t, len(tMsg.Subscriptions) == 1)
			require_Equal(t, tMsg.Subscriptions[0].NoSendReason, "trace Message is not delivered to Clients")
		})
		// The trace message sent is not expected to be received
		_, err = sub.NextMsg(time.Second / 4)
		require_True(t, err == nats.ErrTimeout)
	})
	t.Run("header-jetstream", func(t *testing.T) {
		ncPub, ncSub := createAccountConnPair("js_acc", "js_acc")
		defer ncPub.Close()
		defer ncSub.Close()

		js, err := ncSub.JetStream()
		require_NoError(t, err)

		_, err = js.AddStream(&nats.StreamConfig{Name: "S2", Subjects: []string{"foo"}})
		require_NoError(t, err)
		defer js.DeleteStream("S2")

		sub, err := js.SubscribeSync("foo")
		require_NoError(t, err)
		require_NoError(t, ncSub.Flush())

		msg, err := ncPub.RequestMsg(&nats.Msg{
			Subject: "foo",
			Header: nats.Header{
				TracePing: []string{""},
			}}, time.Second)
		require_NoError(t, err)

		_, err = sub.NextMsg(time.Second / 4)
		require_True(t, err == nats.ErrTimeout)

		_, tMsg := getOkTraceBody(t, msg)

		require_True(t, len(tMsg.Subscriptions) == 1)
		require_Equal(t, tMsg.Subscriptions[0].Acc, "JS_ACC")
		require_Equal(t, tMsg.Subscriptions[0].PubSubject, "foo")
		require_Equal(t, tMsg.Subscriptions[0].Sub, "foo")
		require_Equal(t, tMsg.Subscriptions[0].SubClient.Kind, "JetStream")
		require_Equal(t, tMsg.Subscriptions[0].NoSendReason, "trace Message is not delivered to JetStream")
	})
	t.Run("capture-msg", func(t *testing.T) {
		trcCon, s := createTrcSubExt("acc", fmt.Sprintf(accTrcEvent, "ACC", "capture_msg"))
		defer trcCon.Close()
		ncPub, ncSub := createAccountConnPair("acc", "acc")
		defer ncPub.Close()
		defer ncSub.Close()
		// expecting at least one message, even without subscriber
		data := "somedata"
		_, err := ncPub.RequestMsg(&nats.Msg{
			Subject: "capture",
			Data:    []byte(data),
			Header: nats.Header{
				"hello": []string{"world"},
			}}, time.Second/4)
		require_True(t, err == nats.ErrNoResponders)

		msg, err := s.NextMsg(time.Second)
		require_NoError(t, err)
		_, tMsg := getOkTraceBody(t, msg)

		require_True(t, tMsg.AssociatedInfo != nil)
		require_Equal(t, string(tMsg.AssociatedInfo.Msg), data)
		require_True(t, len(tMsg.AssociatedInfo.Header) == 1)
		require_Equal(t, tMsg.AssociatedInfo.Header.Get("hello"), "world")
		require_Contains(t, tMsg.Reply, "_INBOX.")
		require_TimeoutOnPolicy(s)
	})
	t.Run("mapped-sub", func(t *testing.T) {
		trcCon, s := createTrcSub("map")
		defer trcCon.Close()
		ncPub, ncSub := createAccountConnPair("map", "map")
		defer ncPub.Close()
		defer ncSub.Close()

		sub, err := ncSub.SubscribeSync(">")
		require_NoError(t, err)
		require_NoError(t, ncSub.Flush())

		sendTestData(ncPub, "map.always")
		recvTestData(sub, 1)

		msg, err := s.NextMsg(time.Second)
		require_NoError(t, err)

		_, tMsg := getOkTraceBody(t, msg)

		require_True(t, len(tMsg.Subscriptions) == 1)
		requie_BasicSubNatsClient(t, tMsg.Subscriptions[0], "MAP", ">", "always", "", "")

		requie_BasicPubNatsClient(t, tMsg, "MAP")
		require_Equal(t, tMsg.SubjectMatched, "always")
		require_Equal(t, tMsg.SubjectPreMapping, "map.always")

		require_TimeoutOnPolicy(s)
	})
	t.Run("import-stream", func(t *testing.T) {
		// ensure that accounts only see what they should be seeing
		trcConExp, sExp := createTrcSub("exp_strm")
		defer trcConExp.Close()
		trcConImp, sImp := createTrcSub("imp_strm")
		defer trcConImp.Close()
		ncPub, ncSub := createAccountConnPair("exp_strm", "imp_strm")
		defer ncPub.Close()
		defer ncSub.Close()

		sub1, err := ncSub.SubscribeSync(">")
		require_NoError(t, err)
		require_NoError(t, ncSub.Flush())

		sendTestData(ncPub, "always")
		recvTestData(sub1, 1)

		msgExp, err := sExp.NextMsg(time.Second)
		require_NoError(t, err)
		_, tMsgExp := getOkTraceBody(t, msgExp)

		require_True(t, len(tMsgExp.Subscriptions) == 1)
		require_True(t, *tMsgExp.Subscriptions[0] == TraceSubscription{Acc: "IMP_STRM", ImportSub: "*", Sub: "*", PubSubject: "imported.always"})

		requie_BasicPubNatsClient(t, tMsgExp, "EXP_STRM")
		require_Equal(t, tMsgExp.SubjectMatched, "always")

		require_TimeoutOnPolicy(sExp)

		msgImp, err := sImp.NextMsg(time.Second)
		require_NoError(t, err)
		_, tMsgImp := getOkTraceBody(t, msgImp)

		require_True(t, tMsgImp.Client == nil)

		require_TimeoutOnPolicy(sImp)
		require_AccountOnlyClient(tMsgImp, "EXP_STRM")

		require_True(t, len(tMsgImp.Subscriptions) == 1)
		requie_BasicSubNatsClient(t, tMsgImp.Subscriptions[0], "IMP_STRM", "*", "imported.always", "", "imported.$1")
	})
	t.Run("import-service", func(t *testing.T) {
		trcConExp, sExp := createTrcSub("exp_svc")
		defer trcConExp.Close()
		trcConImp, sImp := createTrcSub("imp_svc")
		defer trcConImp.Close()
		ncPub, ncSub := createAccountConnPair("imp_svc", "exp_svc")
		defer ncPub.Close()
		defer ncSub.Close()

		sub, err := ncSub.SubscribeSync(">")
		require_NoError(t, err)
		require_NoError(t, ncSub.Flush())

		sendTestData(ncPub, "always")
		recvTestData(sub, 1)

		msgExp, err := sExp.NextMsg(time.Second)
		require_NoError(t, err)
		_, tMsgExp := getOkTraceBody(t, msgExp)

		require_True(t, len(tMsgExp.Subscriptions) == 1)
		requie_BasicSubNatsClient(t, tMsgExp.Subscriptions[0], "EXP_SVC", ">", "svc.always", "", "svc.$1")

		require_AccountOnlyClient(tMsgExp, "IMP_SVC")

		require_TimeoutOnPolicy(sExp)

		msgImp, err := sImp.NextMsg(time.Second)
		require_NoError(t, err)

		_, tMsgImp := getOkTraceBody(t, msgImp)
		require_TimeoutOnPolicy(sImp)

		requie_BasicPubNatsClient(t, tMsgImp, "IMP_SVC")
		require_True(t, len(tMsgImp.Subscriptions) == 1)
		require_True(t, *tMsgImp.Subscriptions[0] == TraceSubscription{Acc: "EXP_SVC", ImportSub: "*", Sub: "*", PubSubject: "svc.always"})
		require_TimeoutOnPolicy(sImp)
	})
}

func TestMessageTraceOperatorTrcSubjectOverwrite(t *testing.T) {
	confA := createConfFile(t, []byte(`
		listen: -1
		system_account: SYS
		server_name: srv-A
		operator_msg_trace: {
			trace_name: {Subject: foo, trace_subject "send.trace.here"}
		}
		accounts: {
			SYS: {users: [ {user: sys, password: sys}]}
			ACC: {
				users: [ {user: acc, password: acc}]
				msg_trace: {
					trace_name: {Subject: foo, trace_subject "send.trace.there"}
				}
			}
		}
		no_auth_user: acc
    `))
	defer removeFile(t, confA)
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()

	ncSys := natsConnect(t, sA.ClientURL(), nats.UserInfo("sys", "sys"))
	defer ncSys.Close()

	subSync, err := ncSys.SubscribeSync("send.trace.here")
	require_NoError(t, err)
	require_NoError(t, ncSys.Flush())

	nc := natsConnect(t, sA.ClientURL())
	subAcc, err := nc.SubscribeSync("send.trace.there")
	require_NoError(t, err)
	require_NoError(t, nc.Publish("foo", nil))

	for _, sub := range []*nats.Subscription{subSync, subAcc} {
		msg, err := sub.NextMsg(time.Second)
		require_NoError(t, err)

		_, tMsg := getOkTraceBody(t, msg)
		require_Equal(t, tMsg.SubjectMatched, "foo")
	}
}

func TestMessageTraceFreq(t *testing.T) {
	// ensure enough overlap between account and operator trace such that,
	// even with a margin of error, some messages have to trigger traces by operator and account.
	// this also makes sure tracing messages are not subject to their own tracing rules.
	// Operator can see all traffic, including tracing traffic from accounts
	// Account can sample its tracing traffic, if it did not originate from the same tracing rule.
	confA := createConfFile(t, []byte(`
		listen: -1
		system_account: SYS
		server_name: srv-A
		operator_msg_trace: {
			trace_most_the_time: {Freq: 0.8}
		}
		accounts: {
			SYS: {users: [ {user: sys, password: sys}]}
			ACC: {
				users: [ {user: acc, password: acc}]
				msg_trace: {
					trace_mostly: {Freq: 0.8}
				}
			}
		}
		no_auth_user: acc
    `))
	defer removeFile(t, confA)
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()

	pubCnt := 10000
	subFmt := "pub.%d"

	ncSys := natsConnect(t, sA.ClientURL(), nats.UserInfo("sys", "sys"))
	defer ncSys.Close()
	msgChanSys := make(chan *nats.Msg, 4*pubCnt)
	defer close(msgChanSys)
	sysAcc, err := ncSys.ChanSubscribe(fmt.Sprintf(serverTrcEvent, "*", "trace_most_the_time"), msgChanSys)
	defer sysAcc.Unsubscribe()
	require_NoError(t, err)
	require_NoError(t, ncSys.Flush())

	ncAcc := natsConnect(t, sA.ClientURL(), nats.UserInfo("acc", "acc"))
	defer ncAcc.Close()
	msgChanAcc := make(chan *nats.Msg, pubCnt)
	defer close(msgChanAcc)
	accTrcSubj := fmt.Sprintf(accTrcEvent, "ACC", "trace_mostly")
	subAcc, err := ncAcc.ChanSubscribe(accTrcSubj, msgChanAcc)
	defer subAcc.Unsubscribe()
	require_NoError(t, err)
	require_NoError(t, ncAcc.Flush())

	nc := natsConnect(t, sA.ClientURL())
	for i := 0; i < pubCnt; i++ {
		require_NoError(t, nc.Publish(fmt.Sprintf(subFmt, i), nil))
	}

	// Retrieve account trace messages
	inCnt := 0
	lastIdx := -1
FOR_MSG_ACC:
	for {
		select {
		case m := <-msgChanAcc:
			_, tMsg := getOkTraceBody(t, m)
			var idx int
			inCnt++
			scanCnt, err := fmt.Sscanf(tMsg.SubjectMatched, subFmt, &idx)
			require_NoError(t, err)
			require_True(t, scanCnt == 1)
			require_True(t, idx > lastIdx)
			lastIdx = idx
		case <-time.After(time.Second):
			break FOR_MSG_ACC
		}
	}
	require_True(t, inCnt > ((pubCnt/100*80)-(pubCnt/10))) // half - 10%
	require_True(t, inCnt < ((pubCnt/100*80)+(pubCnt/10))) // half + 10%

	// Retrieve operator trace messages
	inCntAccTrc := 0
	inCntOpTrc := 0
	lastIdxOpTrc := -1
	r1 := regexp.MustCompile(`\$SYS.*ACCOUNT.*CONN`)
	r2 := regexp.MustCompile(`\$SYS.SERVER.*STATSZ`)

FOR_MSG_OP:
	for {
		select {
		case m := <-msgChanSys:
			if _, tMsg := getOkTraceBody(t, m); tMsg.SubjectMatched == accTrcSubj {
				inCntAccTrc++
			} else if r1.MatchString(tMsg.SubjectMatched) || r2.MatchString(tMsg.SubjectMatched) {
				// skip as these are system messages we didn't cause and thus shouldn't count
			} else {
				inCntOpTrc++
				var idx int
				scanCnt, err := fmt.Sscanf(tMsg.SubjectMatched, subFmt, &idx)
				if err != nil {
					require_Contains(t, tMsg.SubjectMatched, "$SYS")
				}
				require_True(t, scanCnt == 1)
				require_True(t, idx > lastIdxOpTrc)
				lastIdxOpTrc = idx
			}
		case <-time.After(time.Second):
			break FOR_MSG_OP
		}
	}
	// Make sure we got 80% of the account traffic
	require_True(t, inCntOpTrc > ((pubCnt/100*80)-(pubCnt/10))) // 80% - 10%
	require_True(t, inCntOpTrc < ((pubCnt/100*80)+(pubCnt/10))) // 80% + 10%
	// Make sure we got 80% of the account tracing traffic
	require_True(t, inCntAccTrc > ((inCnt/100*80)-(inCnt/10))) // 80% - 10%
	require_True(t, inCntAccTrc < ((inCnt/100*80)+(inCnt/10))) // 80% + 10%
}

func TestMessageTraceOperatorCrossServer(t *testing.T) {
	test := func(sA, sB *Server, expectKind string) {
		ncSys := natsConnect(t, sA.ClientURL(), nats.UserInfo("sys", "sys"))
		defer ncSys.Close()
		subTrc1, err := ncSys.SubscribeSync(fmt.Sprintf(serverTrcEvent, "*", "trc1"))
		require_NoError(t, err)
		// Also test for trace accumulation
		subTrc2, err := ncSys.SubscribeSync(fmt.Sprintf(serverTrcEvent, "*", "trc2"))
		require_NoError(t, err)
		require_NoError(t, ncSys.Flush())

		ncB := natsConnect(t, sB.ClientURL())
		defer ncB.Close()
		sub, err := ncB.SubscribeSync("foo")
		require_NoError(t, err)
		require_NoError(t, ncB.Flush())

		ncA := natsConnect(t, sA.ClientURL())
		defer ncA.Close()
		require_NoError(t, ncA.Publish("foo", []byte("helloworld")))

		_, err = sub.NextMsg(time.Second)
		require_NoError(t, err)

		// trc1 observes two messages. one for each server.
		// this is happening despite trc1 not being defined in srv-B
		msgA, err := subTrc1.NextMsg(time.Second)
		require_NoError(t, err)
		siA, m1 := getOkTraceBody(t, msgA)
		require_Equal(t, siA.Name, "srv-A")
		require_Equal(t, siA.ID, sA.ID())
		require_True(t, len(m1.Subscriptions) == 1)
		for _, v := range m1.Subscriptions {
			require_Equal(t, v.SubClient.Kind, expectKind)
			require_Equal(t, v.Acc, "ACC1")
			require_Equal(t, v.SubClient.ServerId, sB.ID())
		}

		msgB, err := subTrc1.NextMsg(time.Second)
		require_NoError(t, err)
		siB, m2 := getOkTraceBody(t, msgB)
		require_Equal(t, siB.Name, "srv-B")
		require_Equal(t, siB.ID, sB.ID())
		require_True(t, len(m2.Subscriptions) == 1)
		for _, v := range m2.Subscriptions {
			require_Equal(t, v.SubClient.Kind, "Client")
		}

		// make suer we only get two
		_, err = subTrc1.NextMsg(time.Second / 4)
		require_True(t, err == nats.ErrTimeout)

		// trace 2 only applies on the leaf node server
		msg2A, err := subTrc2.NextMsg(time.Second)
		require_NoError(t, err)
		si2A, m3 := getOkTraceBody(t, msg2A)
		require_Equal(t, si2A.Name, "srv-B")
		require_Equal(t, si2A.ID, sB.ID())
		require_True(t, len(m3.Subscriptions) == 1)
		for _, v := range m3.Subscriptions {
			require_Equal(t, v.SubClient.Kind, "Client")
		}
		// make sure we only get one
		_, err = subTrc2.NextMsg(time.Second / 4)
		require_True(t, err == nats.ErrTimeout)
	}

	srvATmpl := `
			listen: -1
			system_account: SYS
			server_name: srv-A
			operator_msg_trace: {
				trc1: {Subject: foo}
			}
			accounts: {
				SYS: {users: [ {user: sys, password: sys}]}
				ACC1: {users: [ {user: acc, password: acc}]}
			}
			no_auth_user: acc
`
	srvBTmpl := `
			listen: -1
			system_account: SYS
			server_name: srv-B
			accounts: {
				SYS: {users: [ {user: sys, password: sys}]}
				ACC1: {users: [ {user: acc, password: acc}]}
			}
			no_auth_user: acc
`

	t.Run("leaf", func(t *testing.T) {
		confA := createConfFile(t, []byte(fmt.Sprintf(`
			%s
			leafnodes: {
				listen: localhost:-1
			}
		`, srvATmpl)))
		defer removeFile(t, confA)
		sA, _ := RunServerWithConfig(confA)
		defer sA.Shutdown()

		// because leaf nodes bridge authentication domains, traces will be re-evaluated and have to be explicitly listed
		confB := createConfFile(t, []byte(fmt.Sprintf(`
			%s
			leafnodes: {
				remotes:[{ url:nats://acc:acc@localhost:%d, account: ACC1},
						 { url:nats://sys:sys@localhost:%d, account: SYS}]
			}
			operator_msg_trace: {
				trc1: {Subject: foo}
				trc2: {Subject: foo}
			}
		`, srvBTmpl, sA.opts.LeafNode.Port, sA.opts.LeafNode.Port)))
		defer removeFile(t, confB)
		sB, _ := RunServerWithConfig(confB)
		defer sB.Shutdown()
		checkLeafNodeConnectedCount(t, sB, 2)
		checkLeafNodeConnectedCount(t, sA, 2)

		test(sA, sB, "Leafnode")
	})
	t.Run("route", func(t *testing.T) {
		confA := createConfFile(t, []byte(fmt.Sprintf(`
			%s
			cluster: {
				name: C1
				listen: localhost:-1
			}
		`, srvATmpl)))
		defer removeFile(t, confA)
		sA, _ := RunServerWithConfig(confA)
		defer sA.Shutdown()

		confB := createConfFile(t, []byte(fmt.Sprintf(`
			%s
			cluster: {
				name: C1
				listen: localhost:-1
				routes = [
					nats-route://127.0.0.1:%d
				]
			}
			operator_msg_trace: {
				trc2: {Subject: foo}
			}
		`, srvBTmpl, sA.opts.Cluster.Port)))
		defer removeFile(t, confB)
		sB, _ := RunServerWithConfig(confB)
		defer sB.Shutdown()
		checkClusterFormed(t, sA, sB)

		test(sA, sB, "Router")
	})
	t.Run("gateway", func(t *testing.T) {
		confA := createConfFile(t, []byte(fmt.Sprintf(`
			%s
			gateway: {
				name: GA
				listen: localhost:-1
			}
		`, srvATmpl)))
		defer removeFile(t, confA)
		sA, _ := RunServerWithConfig(confA)
		defer sA.Shutdown()

		confB := createConfFile(t, []byte(fmt.Sprintf(`
			%s
			gateway: {
				name: GB
				listen: localhost:-1
				gateways: [
					{name: "GA", url: "nats://127.0.0.1:%d"},
				]
			}
			operator_msg_trace: {
				trc2: {Subject: foo}
			}
		`, srvBTmpl, sA.opts.Gateway.Port)))
		defer removeFile(t, confB)
		sB, _ := RunServerWithConfig(confB)
		defer sB.Shutdown()
		waitForOutboundGateways(t, sB, 1, 5*time.Second)
		waitForInboundGateways(t, sA, 1, 5*time.Second)
		test(sA, sB, "Gateway")
	})
}

func TestMessageTraceSubjectAccountCrossServer(t *testing.T) {
	test := func(sA, sB *Server, expectKind string) {
		ncB := natsConnect(t, sB.ClientURL())
		defer ncB.Close()
		sub, err := ncB.SubscribeSync("foo")
		require_NoError(t, err)
		//TODO I wonder if I should see queue names in the first trace message from server A
		subq1, err := ncB.QueueSubscribeSync("foo", "q1")
		require_NoError(t, err)
		subq2, err := ncB.QueueSubscribeSync("foo", "q2")
		require_NoError(t, err)
		require_NoError(t, ncB.Flush())

		ncA := natsConnect(t, sA.ClientURL())
		defer ncA.Close()
		ib := nats.NewInbox()
		subTrc, err := ncA.SubscribeSync(ib)
		require_NoError(t, err)
		require_NoError(t, ncA.PublishMsg(&nats.Msg{
			Subject: "foo",
			Reply:   ib,
			Header:  nats.Header{TracePing: []string{""}},
		}))
		_, err = sub.NextMsg(time.Second)
		require_True(t, err == nats.ErrTimeout)
		_, err = subq1.NextMsg(time.Second)
		require_True(t, err == nats.ErrTimeout)
		_, err = subq2.NextMsg(time.Second)
		require_True(t, err == nats.ErrTimeout)

		msgA, err := subTrc.NextMsg(time.Second)
		require_NoError(t, err)
		siA, mA := getOkTraceBody(t, msgA)
		require_Equal(t, siA.Name, "srv-A")
		require_True(t, len(mA.Subscriptions) == 1)
		require_Equal(t, mA.Subscriptions[0].SubClient.ServerId, sB.ID())
		require_Equal(t, mA.Subscriptions[0].NoSendReason, "")
		msgB, err := subTrc.NextMsg(time.Second)
		require_NoError(t, err)
		siB, mB := getOkTraceBody(t, msgB)
		require_Equal(t, siB.Name, "srv-B")
		require_True(t, len(mB.Subscriptions) == 3)
		for _, s := range mB.Subscriptions {
			require_Equal(t, s.NoSendReason, "trace Message is not delivered to Clients")
		}
		// make sure we only get one
		_, err = subTrc.NextMsg(time.Second / 4)
		require_True(t, err == nats.ErrTimeout)

		// test filtering, send so server A, but filter for server B
		require_NoError(t, ncA.PublishMsg(&nats.Msg{
			Subject: "foo",
			Reply:   ib,
			Header:  nats.Header{TracePing: []string{""}},
			Data:    []byte(sB.ID()),
		}))
		msgC, err := subTrc.NextMsg(time.Second)
		require_NoError(t, err)
		siC, _ := getOkTraceBody(t, msgC)
		require_Equal(t, siC.Name, "srv-B")

		_, err = subTrc.NextMsg(time.Second / 4)
		require_True(t, err == nats.ErrTimeout)
	}

	srvATmpl := `
			listen: -1
			system_account: SYS
			server_name: srv-A
			accounts: {
				SYS: {users: [ {user: sys, password: sys}]}
				ACC1: {
					users: [ {user: acc, password: acc}]
					ping_probes: enabled
				}
			}
			no_auth_user: acc
`
	srvBTmpl := `
			listen: -1
			system_account: SYS
			server_name: srv-B
			accounts: {
				SYS: {users: [ {user: sys, password: sys}]}
				ACC1: {
					users: [ {user: acc, password: acc}]
				    ping_probes: enabled
				}
			}
			no_auth_user: acc
`

	t.Run("leaf", func(t *testing.T) {
		confA := createConfFile(t, []byte(fmt.Sprintf(`
			%s
			leafnodes: {
				listen: localhost:-1
			}
		`, srvATmpl)))
		defer removeFile(t, confA)
		sA, _ := RunServerWithConfig(confA)
		defer sA.Shutdown()

		// because leaf nodes bridge authentication domains, traces will be re-evaluated and have to be explicitly listed
		confB := createConfFile(t, []byte(fmt.Sprintf(`
			%s
			leafnodes: {
				remotes:[{ url:nats://acc:acc@localhost:%d, account: ACC1},
						 { url:nats://sys:sys@localhost:%d, account: SYS}]
			}
		`, srvBTmpl, sA.opts.LeafNode.Port, sA.opts.LeafNode.Port)))
		defer removeFile(t, confB)
		sB, _ := RunServerWithConfig(confB)
		defer sB.Shutdown()
		checkLeafNodeConnectedCount(t, sB, 2)
		checkLeafNodeConnectedCount(t, sA, 2)

		test(sA, sB, "Leafnode")
	})
	t.Run("route", func(t *testing.T) {
		confA := createConfFile(t, []byte(fmt.Sprintf(`
			%s
			cluster: {
				name: C1
				listen: localhost:-1
			}
		`, srvATmpl)))
		defer removeFile(t, confA)
		sA, _ := RunServerWithConfig(confA)
		defer sA.Shutdown()

		confB := createConfFile(t, []byte(fmt.Sprintf(`
			%s
			cluster: {
				name: C1
				listen: localhost:-1
				routes = [
					nats-route://127.0.0.1:%d
				]
			}
		`, srvBTmpl, sA.opts.Cluster.Port)))
		defer removeFile(t, confB)
		sB, _ := RunServerWithConfig(confB)
		defer sB.Shutdown()
		checkClusterFormed(t, sA, sB)

		test(sA, sB, "Router")
	})
	t.Run("gateway", func(t *testing.T) {
		confA := createConfFile(t, []byte(fmt.Sprintf(`
			%s
			gateway: {
				name: GA
				listen: localhost:-1
			}
		`, srvATmpl)))
		defer removeFile(t, confA)
		sA, _ := RunServerWithConfig(confA)
		defer sA.Shutdown()

		confB := createConfFile(t, []byte(fmt.Sprintf(`
			%s
			gateway: {
				name: GB
				listen: localhost:-1
				gateways: [
					{name: "GA", url: "nats://127.0.0.1:%d"},
				]
			}
		`, srvBTmpl, sA.opts.Gateway.Port)))
		defer removeFile(t, confB)
		sB, _ := RunServerWithConfig(confB)
		defer sB.Shutdown()
		waitForOutboundGateways(t, sB, 1, 5*time.Second)
		waitForInboundGateways(t, sA, 1, 5*time.Second)
		test(sA, sB, "Gateway")
	})
}

func TestMessageTraceOperatorClusterStream(t *testing.T) {
	confA := createConfFile(t, []byte(`
		listen: -1
		system_account: SYS
		server_name: srv-A
		operator_msg_trace: {
			trc1: {Subject: foo}
		}
		accounts: {
			SYS: {users: [ {user: sys, password: sys}]}
			EXP: {
				users: [ {user: exp, password: exp}]
				exports: [{stream: >}]
			}
			IMP1: {
				users: [ {user: imp1, password: imp1}]
				imports: [{stream: {account: EXP, subject: >}}]
			}
			IMP2: {
				users: [ {user: imp2, password: imp2}]
				imports: [{stream: {account: EXP, subject: >}}]
			}
		}
		no_auth_user: exp
		cluster: {
			name: C1
			listen: localhost:-1
		}
    `))
	defer removeFile(t, confA)
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()

	confB := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		system_account: SYS
		server_name: srv-B
		accounts: {
			SYS: {users: [ {user: sys, password: sys}]}
			EXP: {
				users: [ {user: exp, password: exp}]
				exports: [{stream: >}]
			}
			IMP1: {
				users: [ {user: imp1, password: imp1}]
				imports: [{stream: {account: EXP, subject: >}}]
			}
			IMP2: {
				users: [ {user: imp2, password: imp2}]
				imports: [{stream: {account: EXP, subject: >}}]
			}
		}
		no_auth_user: exp
		cluster: {
			name: C1
			listen: localhost:-1
			routes = [
				nats-route://127.0.0.1:%d
			]
		}
    `, sA.opts.Cluster.Port)))
	defer removeFile(t, confB)
	sB, _ := RunServerWithConfig(confB)
	defer sB.Shutdown()
	checkClusterFormed(t, sA, sB)

	ncSys := natsConnect(t, sA.ClientURL(), nats.UserInfo("sys", "sys"))
	defer ncSys.Close()
	subTrc1, err := ncSys.SubscribeSync(fmt.Sprintf(serverTrcEvent, "*", "trc1"))
	require_NoError(t, err)

	var subs []*nats.Subscription
	for _, s := range []*Server{sA, sB} {
		ncBExp := natsConnect(t, s.ClientURL(), nats.UserInfo("exp", "exp"))
		defer ncBExp.Close()
		subExp1, err := ncBExp.SubscribeSync("*")
		require_NoError(t, err)
		subExp2, err := ncBExp.SubscribeSync("*")
		require_NoError(t, err)
		require_NoError(t, ncBExp.Flush())

		ncBImp1 := natsConnect(t, s.ClientURL(), nats.UserInfo("imp1", "imp1"))
		defer ncBImp1.Close()
		subImp1, err := ncBImp1.SubscribeSync("*")
		require_NoError(t, err)
		subImp2, err := ncBImp1.SubscribeSync("*")
		require_NoError(t, err)
		require_NoError(t, ncBImp1.Flush())

		ncBImp2 := natsConnect(t, s.ClientURL(), nats.UserInfo("imp2", "imp2"))
		defer ncBImp2.Close()
		subImp3, err := ncBImp2.SubscribeSync("*")
		require_NoError(t, err)
		subImp4, err := ncBImp2.SubscribeSync("*")
		require_NoError(t, err)
		require_NoError(t, ncBImp2.Flush())

		subs = append(subs, subExp1, subExp2, subImp1, subImp2, subImp3, subImp4)
	}

	ncA := natsConnect(t, sA.ClientURL())
	defer ncA.Close()
	require_NoError(t, ncA.Publish("foo", []byte("helloworld")))

	for _, sub := range subs {
		_, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
	}
	for _, sub := range subs {
		_, err = sub.NextMsg(time.Second / 4)
		require_True(t, err == nats.ErrTimeout)
	}

	msg1, err := subTrc1.NextMsg(time.Second)
	require_NoError(t, err)
	si1, m1 := getOkTraceBody(t, msg1)
	require_Equal(t, si1.Name, "srv-A")
	require_True(t, len(m1.Subscriptions) == 7)
	routerCnt := 0
	accCnt := map[string]int{}
	for _, v := range m1.Subscriptions {
		if v.SubClient.ServerId == _EMPTY_ {
			require_Equal(t, v.SubClient.Kind, "Client")
			accCnt[v.Acc]++
		} else {
			require_Equal(t, v.SubClient.Kind, "Router")
			require_Equal(t, v.Acc, "EXP")
			require_Equal(t, v.SubClient.ServerId, sB.ID())
			routerCnt++

			_, ok := sA.routes[v.SubClient.ClientId]
			require_True(t, ok)
		}
	}
	require_True(t, len(sA.routes) == 1)
	require_True(t, routerCnt == 1)
	require_True(t, len(accCnt) == 3)
	for _, v := range accCnt {
		require_True(t, v == 2)
	}
	accCnt = map[string]int{}

	msg2, err := subTrc1.NextMsg(time.Second)
	require_NoError(t, err)
	si2, m2 := getOkTraceBody(t, msg2)
	require_Equal(t, si2.Name, "srv-B")
	_, ok := sB.routes[m2.Client.ClientId]
	require_True(t, ok)
	require_True(t, len(sB.routes) == 1)
	require_True(t, len(m2.Subscriptions) == 6)
	for _, v := range m2.Subscriptions {
		require_Equal(t, v.SubClient.Kind, "Client")
		accCnt[v.Acc]++
	}
	require_True(t, len(accCnt) == 3)
	for _, v := range accCnt {
		require_True(t, v == 2)
	}

	// make suer we only get two
	_, err = subTrc1.NextMsg(time.Second / 4)
	require_True(t, err == nats.ErrTimeout)
}

func TestMessageTraceOperatorLeafStream(t *testing.T) {
	// This test is a bit more complex as messages are sent back (after import in B)
	confA := createConfFile(t, []byte(`
		listen: -1
		system_account: SYS
		server_name: srv-A
		operator_msg_trace: {
			trc1: {Subject: foo}
		}
		accounts: {
			SYS: {users: [ {user: sys, password: sys}]}
			EXP: {
				users: [ {user: exp, password: exp}]
				exports: [{stream: foo}]
			}
			IMP1: {
				users: [ {user: imp1, password: imp1}]
				imports: [{stream: {account: EXP, subject: foo}}]
			}
			IMP2: {
				users: [ {user: imp2, password: imp2}]
				imports: [{stream: {account: EXP, subject: foo}}]
			}
		}
		no_auth_user: exp
		leafnodes: {
			listen: localhost:-1
		}
    `))
	defer removeFile(t, confA)
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()

	confB := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		system_account: SYS
		server_name: srv-B
		operator_msg_trace: {
			trc1: {Subject: foo}
		}
		accounts: {
			SYS: {users: [ {user: sys, password: sys}]}
			EXP: {
				users: [ {user: exp, password: exp}]
				exports: [{stream: foo}]
			}
			IMP1: {
				users: [ {user: imp1, password: imp1}]
				imports: [{stream: {account: EXP, subject: foo} }]
			}
			IMP2: {
				users: [ {user: imp2, password: imp2}]
				imports: [{stream: {account: EXP, subject: foo}}]
			}
		}
		no_auth_user: exp
		leafnodes: {
			remotes:[{ url:nats://exp:exp@localhost:%d, account: EXP},
					 { url:nats://sys:sys@localhost:%d, account: SYS},
					 { url:nats://imp1:imp1@localhost:%d, account: IMP1},
					 { url:nats://imp2:imp2@localhost:%d, account: IMP2}]
		}
    `, sA.opts.LeafNode.Port, sA.opts.LeafNode.Port, sA.opts.LeafNode.Port, sA.opts.LeafNode.Port)))
	defer removeFile(t, confB)
	sB, _ := RunServerWithConfig(confB)
	defer sB.Shutdown()
	checkLeafNodeConnectedCount(t, sB, 4)
	checkLeafNodeConnectedCount(t, sA, 4)

	ncSys := natsConnect(t, sA.ClientURL(), nats.UserInfo("sys", "sys"))
	defer ncSys.Close()
	subTrc1, err := ncSys.SubscribeSync(fmt.Sprintf(serverTrcEvent, "*", "trc1"))
	require_NoError(t, err)

	var subs []*nats.Subscription
	for _, s := range []*Server{sA, sB} {
		ncBExp := natsConnect(t, s.ClientURL(), nats.UserInfo("exp", "exp"))
		defer ncBExp.Close()
		subExp1, err := ncBExp.SubscribeSync("*")
		require_NoError(t, err)
		subExp2, err := ncBExp.SubscribeSync("*")
		require_NoError(t, err)
		require_NoError(t, ncBExp.Flush())

		ncBImp1 := natsConnect(t, s.ClientURL(), nats.UserInfo("imp1", "imp1"))
		defer ncBImp1.Close()
		subImp1, err := ncBImp1.SubscribeSync("*")
		require_NoError(t, err)
		subImp2, err := ncBImp1.SubscribeSync("*")
		require_NoError(t, err)
		require_NoError(t, ncBImp1.Flush())

		ncBImp2 := natsConnect(t, s.ClientURL(), nats.UserInfo("imp2", "imp2"))
		defer ncBImp2.Close()
		subImp3, err := ncBImp2.SubscribeSync("*")
		require_NoError(t, err)
		subImp4, err := ncBImp2.SubscribeSync("*")
		require_NoError(t, err)
		require_NoError(t, ncBImp2.Flush())

		// Put in subImp* subscription twice, as messages are duplicated due to
		// EXP being in a remote and importing being processed twice
		subs = append(subs, subExp1, subExp2, subImp1, subImp2, subImp3, subImp4, subImp1, subImp2, subImp3, subImp4)
	}

	ncA := natsConnect(t, sA.ClientURL())
	defer ncA.Close()
	require_NoError(t, ncA.Publish("foo", []byte("helloworld")))

	for _, sub := range subs {
		_, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
	}
	for _, sub := range subs {
		_, err = sub.NextMsg(time.Second / 4)
		require_True(t, err == nats.ErrTimeout)
	}

	msgs := map[string][]*TraceMsg{}
	for i := 0; i < 6; i++ {
		msg1, err := subTrc1.NextMsg(time.Second)
		require_NoError(t, err)
		si, m1 := getOkTraceBody(t, msg1)
		msgs[si.Name] = append(msgs[si.Name], m1)
	}

	m1 := msgs["srv-A"][0]
	require_True(t, len(m1.Subscriptions) == 9)
	leafnodeCnt := 0
	accCnt := map[string]int{}
	for _, v := range m1.Subscriptions {
		if v.SubClient.ServerId == _EMPTY_ {
			require_Equal(t, v.SubClient.Kind, "Client")
		} else {
			require_Equal(t, v.SubClient.Kind, "Leafnode")
			require_Equal(t, v.SubClient.ServerId, sB.ID())
			leafnodeCnt++

			_, ok := sA.leafs[v.SubClient.ClientId]
			require_True(t, ok)
		}
		accCnt[v.Acc]++
	}
	require_True(t, leafnodeCnt == 3)
	require_True(t, len(accCnt) == 3)
	for _, v := range accCnt {
		require_True(t, v == 3)
	}
	accCnt = map[string]int{}

	// These next two messages are incoming from srv-B, a consequence of EXP being in a remote
	// and imported on the other side as well. These two messages are the messages that got imported and then sent back
	for _, m := range msgs["srv-A"][1:] {
		require_Equal(t, m.Client.Kind, "Leafnode")
		_, ok := sA.leafs[m.Client.ClientId]
		require_True(t, ok)
		require_Contains(t, m.Acc, "IMP")
		require_True(t, len(m.Subscriptions) == 2)
		for _, s := range m.Subscriptions {
			require_Equal(t, s.SubClient.Kind, "Client")
			require_Equal(t, s.Acc, m.Acc)
		}
	}

	msgsByAcc := map[string]*TraceMsg{}
	for _, m := range msgs["srv-B"] {
		msgsByAcc[m.Acc] = m
	}
	require_True(t, len(msgsByAcc) == 3)

	for _, m := range msgsByAcc {
		require_Equal(t, m.Client.Kind, "Leafnode")
		_, ok := sB.leafs[m.Client.ClientId]
		require_True(t, ok)
		if m.Acc == "EXP" {
			continue
		}
		require_Contains(t, m.Acc, "IMP")
		require_True(t, len(m.Subscriptions) == 2)
		for _, s := range m.Subscriptions {
			require_Equal(t, s.SubClient.Kind, "Client")
			require_Equal(t, s.Acc, m.Acc)
		}
	}

	m := msgsByAcc["EXP"]
	require_True(t, len(m.Subscriptions) == 8)
	for _, v := range m.Subscriptions {
		accCnt[v.Acc]++
	}
	require_True(t, accCnt["IMP1"] == 3)
	require_True(t, accCnt["IMP2"] == 3)
	require_True(t, accCnt["EXP"] == 2)

	// make suer we only get two
	_, err = subTrc1.NextMsg(time.Second / 4)
	require_True(t, err == nats.ErrTimeout)
}

func TestMessageTraceOperatorClusterService(t *testing.T) {
	confA := createConfFile(t, []byte(`
		listen: -1
		system_account: SYS
		server_name: srv-A
		operator_msg_trace: {
			trc1: {Subject: foo}
		}
		accounts: {
			SYS: {users: [ {user: sys, password: sys}]}
			EXP: {
				users: [ {user: exp, password: exp}]
				exports: [{service: >}]
			}
			IMP: {
				users: [ {user: imp, password: imp}]
				imports: [{service: {account: EXP, subject: >}, to: >}]
			}
		}
		no_auth_user: exp
		cluster: {
			name: C1
			listen: localhost:-1
		}
    `))
	defer removeFile(t, confA)
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()

	confB := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		system_account: SYS
		server_name: srv-B
		operator_msg_trace: {
			trc1: {Subject: foo, Probes: accountz}
		}
		accounts: {
			SYS: {users: [ {user: sys, password: sys}]}
			EXP: {
				users: [ {user: exp, password: exp}]
				exports: [{service: >}]
			}
			IMP: {
				users: [ {user: imp, password: imp}]
				imports: [{service: {account: EXP, subject: >}}]
			}
		}
		no_auth_user: exp
		cluster: {
			name: C1
			listen: localhost:-1
			routes = [
				nats-route://127.0.0.1:%d
			]
		}
    `, sA.opts.Cluster.Port)))
	defer removeFile(t, confB)
	sB, _ := RunServerWithConfig(confB)
	defer sB.Shutdown()
	checkClusterFormed(t, sA, sB)

	ncSys := natsConnect(t, sA.ClientURL(), nats.UserInfo("sys", "sys"))
	defer ncSys.Close()
	subTrc1, err := ncSys.SubscribeSync(fmt.Sprintf(serverTrcEvent, "*", "trc1"))
	require_NoError(t, err)

	var subs []*nats.Subscription
	for _, s := range []*Server{sA, sB} {
		ncBExp := natsConnect(t, s.ClientURL(), nats.UserInfo("exp", "exp"))
		defer ncBExp.Close()
		subExp1, err := ncBExp.SubscribeSync("*")
		require_NoError(t, err)
		require_NoError(t, ncBExp.Flush())

		ncBImp1 := natsConnect(t, s.ClientURL(), nats.UserInfo("imp", "imp"))
		defer ncBImp1.Close()
		subImp1, err := ncBImp1.SubscribeSync("*")
		require_NoError(t, err)

		subs = append(subs, subExp1, subImp1)
	}

	ncA := natsConnect(t, sA.ClientURL(), nats.UserInfo("imp", "imp"))
	defer ncA.Close()
	require_NoError(t, ncA.Publish("foo", []byte("helloworld")))

	for _, sub := range subs {
		_, err = sub.NextMsg(time.Second)
		require_NoError(t, err)
	}
	for _, sub := range subs {
		_, err = sub.NextMsg(time.Second / 4)
		require_True(t, err == nats.ErrTimeout)
	}

	msg1, err := subTrc1.NextMsg(time.Second)
	require_NoError(t, err)
	si1, m1 := getOkTraceBody(t, msg1)
	require_Equal(t, si1.Name, "srv-A")
	require_True(t, len(m1.Subscriptions) == 4)
	routerCnt := 0
	accCnt := map[string]int{}
	for _, v := range m1.Subscriptions {
		if v.SubClient.ServerId == _EMPTY_ {
			require_Equal(t, v.SubClient.Kind, "Client")
		} else {
			require_Equal(t, v.SubClient.Kind, "Router")
			require_Equal(t, v.SubClient.ServerId, sB.ID())
			routerCnt++

			_, ok := sA.routes[v.SubClient.ClientId]
			require_True(t, ok)
		}
		accCnt[v.Acc]++
	}
	require_True(t, len(sA.routes) == 1)
	require_True(t, routerCnt == 2)
	require_True(t, len(accCnt) == 2)
	for _, v := range accCnt {
		require_True(t, v == 2)
	}
	accCnt = map[string]int{}

	msg2, err := subTrc1.NextMsg(time.Second)
	require_NoError(t, err)
	si2, m2 := getOkTraceBody(t, msg2)
	require_Equal(t, si2.Name, "srv-B")
	_, ok := sB.routes[m2.Client.ClientId]
	require_True(t, ok)
	require_True(t, len(sB.routes) == 1)
	require_True(t, len(m2.Subscriptions) == 1)
	for _, v := range m2.Subscriptions {
		require_Equal(t, v.SubClient.Kind, "Client")
		accCnt[v.Acc]++
	}

	// make suer we only get two
	msg3, err := subTrc1.NextMsg(time.Second / 4)
	require_NoError(t, err)
	si3, m3 := getOkTraceBody(t, msg3)
	require_Equal(t, si3.Name, "srv-B")
	_, ok = sB.routes[m3.Client.ClientId]
	require_True(t, ok)
	require_True(t, len(sB.routes) == 1)
	require_True(t, len(m3.Subscriptions) == 1)
	for _, v := range m3.Subscriptions {
		require_Equal(t, v.SubClient.Kind, "Client")
		require_Equal(t, v.Sub, "*")
		accCnt[v.Acc]++
	}

	// Ensure that there was a subscribing client from each account
	require_True(t, len(accCnt) == 2)
	for _, v := range accCnt {
		require_True(t, v == 1)
	}

	// make suer we only get three
	_, err = subTrc1.NextMsg(time.Second / 4)
	require_True(t, err == nats.ErrTimeout)
}

func TestMessageTraceAccountHeaderLeafStream(t *testing.T) {
	// This test is a bit more complex as messages are sent back (after import in B)
	confA := createConfFile(t, []byte(`
		listen: -1
		system_account: SYS
		server_name: srv-A
		accounts: {
			SYS: {users: [ {user: sys, password: sys}]}
			EXP1: {
				users: [ {user: exp, password: exp}]
				exports: [{stream: foo}]
				ping_probes: enabled
			}
			IMP1: {
				users: [ {user: imp1, password: imp1}]
				imports: [{stream: {account: EXP1, subject: foo}}]
			}
		}
		no_auth_user: exp
		leafnodes: {
			listen: localhost:-1
		}
    `))
	defer removeFile(t, confA)
	sA, _ := RunServerWithConfig(confA)
	defer sA.Shutdown()

	confB := createConfFile(t, []byte(fmt.Sprintf(`
		listen: -1
		system_account: SYS
		server_name: srv-B
		accounts: {
			SYS: {users: [ {user: sys, password: sys}]}
			EXP2: {
				users: [ {user: exp, password: exp}]
				exports: [{stream: foo}]
				ping_probes: enabled
			}
			IMP2: {
				users: [ {user: imp1, password: imp1}]
				imports: [{stream: {account: EXP2, subject: foo} }]
			}
		}
		no_auth_user: exp
		leafnodes: {
			remotes:[{ url:nats://exp:exp@localhost:%d, account: EXP2},
					 { url:nats://sys:sys@localhost:%d, account: SYS}]
		}
    `, sA.opts.LeafNode.Port, sA.opts.LeafNode.Port)))
	defer removeFile(t, confB)
	sB, _ := RunServerWithConfig(confB)
	defer sB.Shutdown()
	checkLeafNodeConnectedCount(t, sB, 2)
	checkLeafNodeConnectedCount(t, sA, 2)

	ncTrc := natsConnect(t, sA.ClientURL())
	defer ncTrc.Close()
	ib := nats.NewInbox()
	subTrc, err := ncTrc.SubscribeSync(ib)
	require_NoError(t, err)
	require_NoError(t, ncTrc.Flush())

	var subs []*nats.Subscription
	for _, s := range []*Server{sA, sB} {
		ncBExp := natsConnect(t, s.ClientURL(), nats.UserInfo("exp", "exp"))
		defer ncBExp.Close()
		subExp1, err := ncBExp.SubscribeSync("*")
		require_NoError(t, err)
		require_NoError(t, ncBExp.Flush())

		ncBImp1 := natsConnect(t, s.ClientURL(), nats.UserInfo("imp1", "imp1"))
		defer ncBImp1.Close()
		subImp1, err := ncBImp1.SubscribeSync("*")
		require_NoError(t, err)
		require_NoError(t, ncBImp1.Flush())

		// Put in subImp* subscription twice, as messages are duplicated due to
		// EXP being in a remote and importing being processed twice
		subs = append(subs, subExp1, subImp1)
	}

	ncA := natsConnect(t, sA.ClientURL())
	defer ncA.Close()
	require_NoError(t, ncA.PublishMsg(&nats.Msg{
		Subject: "foo",
		Reply:   ib,
		Header: nats.Header{
			TracePing: []string{""},
		}}))
	require_NoError(t, ncA.Flush())

	// ensure the message is not received
	for _, sub := range subs {
		_, err := sub.NextMsg(time.Second / 4)
		require_True(t, err == nats.ErrTimeout)
	}

	// test both responses
	msgRef := _EMPTY_
	msg1, err := subTrc.NextMsg(time.Second)
	require_NoError(t, err)
	si1, m1 := getOkTraceBody(t, msg1)
	require_Equal(t, si1.Name, "srv-A")
	require_Equal(t, m1.Acc, "EXP1")
	require_Equal(t, m1.Client.Kind, "Client")
	require_True(t, len(m1.Subscriptions) == 3)
	for _, s := range m1.Subscriptions {
		switch s.Acc {
		case "IMP1":
			require_Equal(t, s.NoSendReason, "trace Message is not delivered to foreign account")
		case "EXP1":
			if s.SubClient.Kind == "Leafnode" {
				msgRef = s.Ref
				require_Equal(t, s.NoSendReason, "")
			} else {
				require_Equal(t, s.NoSendReason, "trace Message is not delivered to Clients")
			}
		default:
			require_True(t, false)
		}
	}
	msg2, err := subTrc.NextMsg(time.Second)
	require_NoError(t, err)
	si2, m2 := getOkTraceBody(t, msg2)
	require_Equal(t, si2.Name, "srv-B")
	require_Equal(t, m2.Acc, "EXP2")
	require_Equal(t, m2.Client.Kind, "Leafnode")
	require_Equal(t, m2.Client.ServerId, sA.ID())
	require_Equal(t, m2.MsgRef, msgRef)
	require_True(t, len(m2.Subscriptions) == 2)
	for _, s := range m2.Subscriptions {
		switch s.Acc {
		case "IMP2":
			require_Equal(t, s.NoSendReason, "trace Message is not delivered to foreign account")
		case "EXP2":
			require_Equal(t, s.NoSendReason, "trace Message is not delivered to Clients")
		default:
			require_True(t, false)
		}
	}
	// make suer we only get two
	_, err = subTrc.NextMsg(time.Second / 4)
	require_True(t, err == nats.ErrTimeout)
}

func TestMessageTraceAccountJSCluster(t *testing.T) {
	rtUrlFmt := "nats-route://127.0.0.1:%d, nats-route://127.0.0.1:%d"
	srv := []*Server{}
	for _, v := range []struct {
		name  string
		rport int
		rUrls string
	}{{"srv-A", 7222, fmt.Sprintf(rtUrlFmt, 7223, 7224)},
		{"srv-B", 7223, fmt.Sprintf(rtUrlFmt, 7222, 7224)},
		{"srv-C", 7224, fmt.Sprintf(rtUrlFmt, 7222, 7223)}} {
		storeDir := createDir(t, JetStreamStoreDir)
		conf := createConfFile(t, []byte(fmt.Sprintf(`
			listen: -1
			system_account: SYS
			server_name: %s
			jetstream: { max_mem_store: 10Mb, max_file_store: 10Mb, store_dir: %s }
			accounts: {
				SYS: { users: [ {user: sys, password: sys}]}
				ACC: {
					users: [ {user: acc, password: acc}]
					jetstream: enabled
					msg_trace: { all: {Subject: foo } }
				}
			}
			no_auth_user = acc
			cluster: { name: cluster, listen: 127.0.0.1:%d, routes = [%s]}
		`, v.name, storeDir, v.rport, v.rUrls)))
		defer removeFile(t, conf)
		s, _ := RunServerWithConfig(conf)
		defer s.Shutdown()
		srv = append(srv, s)
	}
	checkClusterFormed(t, srv...)
	checkForJSClusterUp(t, srv...)

	trcCon := natsConnect(t, srv[0].ClientURL())
	trcSub, err := trcCon.SubscribeSync(fmt.Sprintf(accTrcEvent, "ACC", "all"))
	require_NoError(t, err)

	js, err := trcCon.JetStream()
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{Name: "STREAM", Subjects: []string{"foo"}, Replicas: 3})
	require_NoError(t, err)
	defer js.DeleteStream("S2")
	_, err = js.AddConsumer("STREAM", &nats.ConsumerConfig{Durable: "DURABLE", DeliverSubject: "deliver"})
	require_NoError(t, err)

	var nonLdrSrv *Server
	for _, v := range srv {
		if v.JetStreamIsStreamLeader("ACC", "STREAM") ||
			v.JetStreamIsConsumerLeader("ACC", "STREAM", "DURABLE") {
			continue
		}
		nonLdrSrv = v
		break
	}

	testCon := natsConnect(t, nonLdrSrv.ClientURL())
	defer testCon.Close()
	require_NoError(t, testCon.Publish("foo", nil))

	msgs := map[string][]*TraceMsg{}
	for i := 0; i < 5; i++ {
		msg1, err := trcSub.NextMsg(time.Second)
		require_NoError(t, err)
		_, m := getOkTraceBody(t, msg1)
		msgs[m.MsgRef] = append(msgs[m.MsgRef], m)
	}
	_, err = trcSub.NextMsg(time.Second)
	require_True(t, err == nats.ErrTimeout)

	js, err = testCon.JetStream()
	require_NoError(t, err)
	sub, err := js.SubscribeSync("foo", nats.Durable("DURABLE"))
	require_NoError(t, err)
	_, err = sub.NextMsg(time.Second)
	require_NoError(t, err)

	for i := 0; i < 2; i++ {
		msg1, err := trcSub.NextMsg(time.Second)
		require_NoError(t, err)
		_, m := getOkTraceBody(t, msg1)
		msgs[m.MsgRef] = append(msgs[m.MsgRef], m)
	}
	_, err = trcSub.NextMsg(time.Second)
	require_True(t, err == nats.ErrTimeout)

	require_True(t, len(msgs[""]) == 1)
	m1 := msgs[""][0]
	require_Equal(t, m1.Client.Kind, "Client")
	require_Equal(t, m1.Subscriptions[0].SubClient.Kind, "Router")
	require_True(t, len(msgs[m1.Subscriptions[0].Ref]) == 1)
	m2 := msgs[m1.Subscriptions[0].Ref][0]
	require_Equal(t, m2.Client.Kind, "Router")
	require_Equal(t, m2.Subscriptions[0].SubClient.Kind, "JetStream")
	require_True(t, len(msgs[m2.Subscriptions[0].Ref]) == 3)
	// check stream
	storeCnt := 0
	var m3 *TraceMsg
	for _, m := range msgs[m2.Subscriptions[0].Ref] {
		require_Equal(t, m.Client.Kind, "JetStream")
		require_Equal(t, m.Stores[0].StreamRef, "STREAM")
		if m.Stores[0].DidStore {
			storeCnt++
		}
		if m, ok := msgs[m.Stores[0].Ref]; ok {
			m3 = m[0]
		}
	}
	require_True(t, storeCnt == 3)
	// check stream/consumer
	require_Equal(t, m3.Subscriptions[0].SubClient.Kind, "Router")
	require_True(t, len(msgs[m3.Subscriptions[0].Ref]) == 1)
	require_Equal(t, m3.StreamRef, "STREAM")
	require_Equal(t, m3.ConsumerRef, "DURABLE")

	m4 := msgs[m3.Subscriptions[0].Ref][0]
	require_Equal(t, m4.Client.Kind, "Router")
	require_Equal(t, m4.Subscriptions[0].SubClient.Kind, "Client")

	require_True(t, m1.Client.ClientId == m4.Subscriptions[0].SubClient.ClientId)
}

func TestMessageTraceAccountJSMirror(t *testing.T) {
	rtUrlFmt := "nats-route://127.0.0.1:%d, nats-route://127.0.0.1:%d"
	srv := []*Server{}
	for _, v := range []struct {
		name  string
		rport int
		rUrls string
	}{{"srv-A", 7222, fmt.Sprintf(rtUrlFmt, 7223, 7224)},
		{"srv-B", 7223, fmt.Sprintf(rtUrlFmt, 7222, 7224)},
		{"srv-C", 7224, fmt.Sprintf(rtUrlFmt, 7222, 7223)}} {
		storeDir := createDir(t, JetStreamStoreDir)
		conf := createConfFile(t, []byte(fmt.Sprintf(`
			listen: -1
			system_account: SYS
			server_name: %s
			jetstream: { max_mem_store: 10Mb, max_file_store: 10Mb, store_dir: %s }
			accounts: {
				SYS: { users: [ {user: sys, password: sys}]}
				ACC: {
					users: [ {user: acc, password: acc}]
					jetstream: enabled
					msg_trace: { all: {Subject: foo } }
				}
			}
			no_auth_user = acc
			cluster: { name: cluster, listen: 127.0.0.1:%d, routes = [%s]}
		`, v.name, storeDir, v.rport, v.rUrls)))
		defer removeFile(t, conf)
		s, _ := RunServerWithConfig(conf)
		defer s.Shutdown()
		srv = append(srv, s)
	}
	checkClusterFormed(t, srv...)
	checkForJSClusterUp(t, srv...)

	trcCon := natsConnect(t, srv[0].ClientURL())
	trcSub, err := trcCon.SubscribeSync(fmt.Sprintf(accTrcEvent, "ACC", "all"))
	require_NoError(t, err)

	for _, s := range srv {
		checkSubInterest(t, s, "ACC", fmt.Sprintf(accTrcEvent, "ACC", "all"), time.Second)
	}

	js, err := trcCon.JetStream()
	require_NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{Name: "STREAM", Subjects: []string{"foo"}, Replicas: 1})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "MIRROR", Mirror: &nats.StreamSource{Name: "STREAM"}, Replicas: 1})
	require_NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{Name: "SOURCE", Sources: []*nats.StreamSource{{Name: "MIRROR"}}, Replicas: 1})
	require_NoError(t, err)

	var ldrSrv *Server
	for _, v := range srv {
		if v.JetStreamIsStreamLeader("ACC", "STREAM") {
			ldrSrv = v
			break
		}
	}

	require_NoError(t, trcCon.Flush())

	testCon := natsConnect(t, ldrSrv.ClientURL(), nats.Name("TESTNAME"))
	defer testCon.Close()
	testCon.SubscribeSync("foo")
	require_NoError(t, testCon.Flush())
	require_NoError(t, testCon.Publish("foo", nil))

	// Because this is a cluster, streams may be located on the same server or not.
	// This is why no explicit message count is coded.
	msgs := map[string]*TraceMsg{}
	for i := 0; i < 7; i++ {
		msg1, err := trcSub.NextMsg(time.Second)
		if err == nats.ErrTimeout {
			break
		}
		_, m := getOkTraceBody(t, msg1)
		msgs[m.MsgRef] = m
	}
	consumers := 0
	streams := map[string]struct{}{}
	for _, v := range msgs {
		if len(v.Stores) == 1 && v.Stores[0].DidStore {
			streams[v.Stores[0].StreamRef] = struct{}{}
		} else if v.ConsumerRef != _EMPTY_ {
			consumers++
		}
	}
	require_True(t, len(streams) == 3)
	require_True(t, consumers == 2)
}
