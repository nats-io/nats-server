package stree

import (
        "fmt"
        "encoding/json"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
        //"capnproto.org/go/capnp/v3"
)

type Serializer interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSONSerializer implements Serializer using JSON.
type JSONSerializer struct{}

func (s JSONSerializer) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (s JSONSerializer) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// ProtobufSerializer implements Serializer using Protocol Buffers.
type ProtobufSerializer struct{}

func (s ProtobufSerializer) Marshal(v any) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("value is not a proto.Message")
	}
	anyValue, err := anypb.New(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Any proto: %w", err)
	}
	return proto.Marshal(anyValue)
}

func (s ProtobufSerializer) Unmarshal(data []byte, v any) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("value is not a proto.Message")
	}
	anyValue := &anypb.Any{}
	if err := proto.Unmarshal(data, anyValue); err != nil {
		return fmt.Errorf("failed to unmarshal Any proto: %w", err)
	}
	return anyValue.UnmarshalTo(msg)
}

// CapnpSerializer implements Serializer using Cap'n Proto.
//type CapnpSerializer struct{}
//
//func (s CapnpSerializer) Marshal(v any) ([]byte, error) {
//	msg, ok := v.(capnp.Struct)
//	if !ok {
//		return nil, fmt.Errorf("value is not a capnp.Struct")
//	}
//	return msg.Marshal()
//}
//
//func (s CapnpSerializer) Unmarshal(data []byte, v any) error {
//	msg, ok := v.(*capnp.Message)
//	if !ok {
//		return fmt.Errorf("value is not a *capnp.Message")
//	}
//	_, err := msg.Unmarshal(data)
//	return err
//}
