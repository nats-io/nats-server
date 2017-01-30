package extension

import (
	"strings"
	"reflect"
)

type Permission interface {
	RegisterSubscribePermission(clientExtension map[string]string, authenticator []string, subscribe []string)
	RegisterPublishPermission(clientExtension map[string]string, authenticator []string, publish []string)
	CheckSubscribe(clientExtension map[string]string, subject string) bool
	CheckPublish(clientExtension map[string]string, subject string) bool
}

func RegisterPermission(clientExtension map[string]string, authenticator []string, publish []string, subscribe []string){
	golangType := publish[0]
	if strings.HasPrefix(golangType, "golang_type."){
		golangType = strings.Replace(golangType, "golang_type.", "", 1)

		clientExtension["golang_type_pub"] = golangType

		v := reflect.New(GetTypeRegistry()[golangType])

		params := make([]reflect.Value, 3)
		params[0] = reflect.ValueOf(clientExtension)
		params[1] = reflect.ValueOf(authenticator)
		params[2] = reflect.ValueOf(publish)
		v.MethodByName("RegisterPublishPermission").Call(params)
	}

	golangType = subscribe[0]
	if strings.HasPrefix(golangType, "golang_type."){
		golangType = strings.Replace(golangType, "golang_type.", "", 1)

		clientExtension["golang_type_sub"] = golangType

		v := reflect.New(GetTypeRegistry()[golangType])

		params := make([]reflect.Value, 3)
		params[0] = reflect.ValueOf(clientExtension)
		params[1] = reflect.ValueOf(authenticator)
		params[2] = reflect.ValueOf(subscribe)
		v.MethodByName("RegisterSubscribePermission").Call(params)
	}
}

func CheckPublishPermission(clientExtension map[string]string, subject string) bool {
	if clientExtension["golang_type_pub"] == "" {
		return false
	}

	golangType := clientExtension["golang_type_pub"]

	v := reflect.New(GetTypeRegistry()[golangType])

	params := make([]reflect.Value, 2)
	params[0] = reflect.ValueOf(clientExtension)
	params[1] = reflect.ValueOf(subject)
	result := v.MethodByName("CheckPublish").Call(params)
	return result[0].Interface().(bool)
}

func CheckSubscribePermission(clientExtension map[string]string, subject string) bool {
	if clientExtension["golang_type_sub"] == "" {
		return false
	}

	golangType := clientExtension["golang_type_sub"]

	v := reflect.New(GetTypeRegistry()[golangType])

	params := make([]reflect.Value, 2)
	params[0] = reflect.ValueOf(clientExtension)
	params[1] = reflect.ValueOf(subject)
	result := v.MethodByName("CheckSubscribe").Call(params)
	return result[0].Interface().(bool)
}