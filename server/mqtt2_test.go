package server

import (
	"fmt"
	"testing"

	"github.com/nats-io/nuid"
)

func TestMQTTNewSubRetainedRace(t *testing.T) {
	pubTopic := "/bar"

	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	for _, subTopic := range []string{"#", "/#", "/bar"} {
		t.Run(subTopic, func(t *testing.T) {
			for _, qos := range []byte{0, 1, 2} {
				t.Run(fmt.Sprintf("QOS%d", qos), func(t *testing.T) {
					testMQTTNewSubRetainedRace(t, o, subTopic, pubTopic, qos)
				})
			}
		})
	}
}

func testMQTTNewSubRetainedRace(t *testing.T, o *Options, subTopic, pubTopic string, QOS byte) {
	expectedFlags := (QOS << 1) | mqttPubFlagRetain
	payload := []byte("testmsg")

	pubID := nuid.Next()
	pubc, pubr := testMQTTConnectRetry(t, &mqttConnInfo{clientID: pubID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port, 3)
	testMQTTCheckConnAck(t, pubr, mqttConnAckRCConnectionAccepted, false)
	defer testMQTTDisconnectEx(t, pubc, nil, true)
	defer pubc.Close()
	testMQTTPublish(t, pubc, pubr, QOS, false, true, pubTopic, 1, payload)

	subID := nuid.Next()
	subc, subr := testMQTTConnect(t, &mqttConnInfo{clientID: subID, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	testMQTTCheckConnAck(t, subr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, subc, subr, []*mqttFilter{{filter: subTopic, qos: QOS}}, []byte{QOS})

	testMQTTCheckPubMsg(t, subc, subr, pubTopic, expectedFlags, payload)

	// Disconnect and connect again
	testMQTTDisconnectEx(t, subc, nil, true)
	subc.Close()
}
