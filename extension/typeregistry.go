package extension

import (
	"reflect"
	"sync"

	"github.com/nats-io/gnatsd/extension/remote"
)

var typeRegistry map[string]reflect.Type
var typeRegistryOnce sync.Once

func GetTypeRegistry() map[string]reflect.Type {
	typeRegistryOnce.Do(func(){
		typeRegistry = make(map[string]reflect.Type)
		typeRegistry["remote.RemoteAuth"] = reflect.TypeOf(remote.RemoteAuth{})
		typeRegistry["remote.RemotePermission"] = reflect.TypeOf(remote.RemotePermission{})
	})
	return typeRegistry
}