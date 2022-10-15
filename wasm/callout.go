package main

import (
	"fmt"
	"reflect"
	"unsafe"
)

//export callout
func callout(echo bool) bool {
	return !echo
}

//export add
func add(x, y uint32) uint32 {
	return x + y
}

func transform(subject string) string {
	return fmt.Sprintf("transformed.%s", subject)
}

//export transform
func _transform(ptr, size uint32) (ptrSize uint64) {
	subject := ptrToString(ptr, size)
	transformed := transform(subject)
	ptr, size = stringToPtr(transformed)
	return (uint64(ptr) << uint64(32)) | uint64(size)
}

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

// ptrToString returns a string from WebAssembly compatible numeric types
// representing its pointer and length.
func ptrToString(ptr uint32, size uint32) string {
	// Get a slice view of the underlying bytes in the stream. We use SliceHeader, not StringHeader
	// as it allows us to fix the capacity to what was allocated.
	return *(*string)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(ptr),
		Len:  uintptr(size), // Tinygo requires these as uintptrs even if they are int fields.
		Cap:  uintptr(size), // ^^ See https://github.com/tinygo-org/tinygo/issues/1284
	}))
}

// stringToPtr returns a pointer and size pair for the given string in a way
// compatible with WebAssembly numeric types.
func stringToPtr(s string) (uint32, uint32) {
	buf := []byte(s)
	ptr := &buf[0]
	unsafePtr := uintptr(unsafe.Pointer(ptr))
	return uint32(unsafePtr), uint32(len(buf))
}

func main() {
}
