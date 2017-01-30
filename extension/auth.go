package extension

import (
	"strings"
	"reflect"
	"fmt"
)

type Auth interface {
	Check(authenticator []string, username string, password string, clientToken string) (bool)
}

func CheckExternalUser(authenticator []string, username string, password string, clientToken string) ([]string, bool){
	if authenticator == nil || authenticator[0] == "" || !strings.HasPrefix(authenticator[0], "golang_type"){
		fmt.Errorf("User authenticator or golang_type can not be nil")
		return nil, false
	}

	newAuthenticator := make([]string, len(authenticator))
	copy(newAuthenticator, authenticator)

	golangType := strings.Replace(newAuthenticator[0], "golang_type.", "", 1)

	v := reflect.New(GetTypeRegistry()[golangType])

	params := make([]reflect.Value, 4)
	params[0] = reflect.ValueOf(newAuthenticator)
	params[1] = reflect.ValueOf(username)
	params[2] = reflect.ValueOf(password)
	params[3] = reflect.ValueOf(clientToken)
	result := v.MethodByName("Check").Call(params)

	return newAuthenticator, result[0].Interface().(bool)
}
