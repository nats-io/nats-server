package remote

import (
	"encoding/json"
	"net/http"

	"bytes"
	"fmt"
)

type RemoteAuth struct {
}

func (t *RemoteAuth) Check(authenticator []string, username string, password string, clientToken string) bool {
	url := authenticator[1]

	var values map[string]string
	if clientToken != "" {
		values = map[string]string{"token": clientToken}
	} else {
		values = map[string]string{"username": username, "password": password}
	}

	jsonValue, _ := json.Marshal(values)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonValue))

	if err != nil {
		fmt.Println("authAuthenticatorRequest Error: ", err)
		return false
	}

	defer resp.Body.Close()

	authResponse := make(map[string]string)
	if err := json.NewDecoder(resp.Body).Decode(&authResponse); err != nil {
		fmt.Println("Authenticate result parse Failed ", err)
		return false
	}

	if resp.StatusCode != 200 || authResponse["token"] == "" {
		fmt.Println("authAuthenticatorRequest Failed")
		return false
	}

	authenticator = append(authenticator, authResponse["token"])

	return true
}
