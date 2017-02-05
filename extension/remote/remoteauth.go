package remote

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"bytes"
	"fmt"
)

type RemoteAuth struct {
}

func (t *RemoteAuth) Check(authenticator []string, username string, password string, clientToken string) error {
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
		return fmt.Errorf("authAuthenticatorRequest Error: %v", err)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("authAuthenticatorRequest Responese Body Error: %v", err)
	}

	fmt.Println("authAuthenticatorRequest Body: " + string(body))

	authResponse := make(map[string]string)
	err = json.Unmarshal(body, &authResponse)
	if err != nil {
		return fmt.Errorf("authAuthenticatorRequest Json Parse Error: %v", err)
	}
	fmt.Println("authAuthenticatorRequest response error: " + string(resp.StatusCode))
	if resp.StatusCode != 200 || authResponse["token"] == "" {
		return fmt.Errorf("authAuthenticatorRequest Failed")
	}
	return nil
}
