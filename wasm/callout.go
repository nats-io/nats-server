package main

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/buger/jsonparser"
)

//export callout
func callout(echo bool) bool {
	return !echo
}

//export add
func add(x, y uint32) uint32 {
	return x + y
}

func transform(message Message) Message {
	return Message{
		Subject: message.Subject,
		Reply:   message.Reply,
		Message: message.Message,
	}
}

//export transform
func _transform(ptr, size uint32) (ptrSize uint64) {
	message := ptrToBytes(ptr, size)
	var msg Message
	subject, err := jsonparser.GetString(message, "Subject")
	if err != nil {
		panic(err)
	}
	msg.Subject = subject
	reply, err := jsonparser.GetString(message, "Reply")
	if err != nil {
		panic(err)
	}
	msg.Reply = reply
	payload, err := jsonparser.GetString(message, "Message")
	if err != nil {
		panic(err)
	}
	pp, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		panic(err)
	}
	msg.Message = pp

	transformed := transform(msg)
	ptr, size = bytesToPtr(Marshal(transformed))
	return (uint64(ptr) << uint64(32)) | uint64(size)
}

func Marshal(msg Message) []byte {
	paylaod := base64.StdEncoding.EncodeToString(msg.Message)
	return []byte(fmt.Sprintf(`{"subject": "%s","reply":"%s","message":"%s"}`, msg.Subject, msg.Reply, paylaod))
}

// func message_process(ptr, size uint32) (ptrSize uint64) {

// }

// // ptrToString returns a string from WebAssembly compatible numeric types
// // representing its pointer and length.
// func ptrToString(ptr int, size int) string {
// 	// Get a slice view of the underlying bytes in the stream. We use SliceHeader, not StringHeader
// 	// as it allows us to fix the capacity to what was allocated.
// 	return *(*string)(unsafe.Pointer(&reflect.SliceHeader{
// 		Data: uintptr(ptr),
// 		Len:  size, // Tinygo requires these as uintptrs even if they are int fields.
// 		Cap:  size, // ^^ See https://github.com/tinygo-org/tinygo/issues/1284
// 	}))
// }

// ptrToBytes returns a string from WebAssembly compatible numeric types
// representing its pointer and length.
func ptrToBytes(ptr uint32, size uint32) []byte {
	// Get a slice view of the underlying bytes in the stream. We use SliceHeader, not StringHeader
	// as it allows us to fix the capacity to what was allocated.
	bts := *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(ptr),
		Len:  uintptr(size), // Tinygo requires these as uintptrs even if they are int fields.
		Cap:  uintptr(size), // ^^ See https://github.com/tinygo-org/tinygo/issues/1284
	}))
	return []byte(bts)
}

// bytesToPtr returns a pointer and size pair for the given string in a way
// compatible with WebAssembly numeric types.
func bytesToPtr(s []byte) (uint32, uint32) {
	buf := []byte(s)
	ptr := &buf[0]
	unsafePtr := uintptr(unsafe.Pointer(ptr))
	return uint32(unsafePtr), uint32(len(buf))
}

func main() {
}
