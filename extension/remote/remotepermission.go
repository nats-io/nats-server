package remote

import (
	"io/ioutil"
	"net/http"
	"fmt"
)

type RemotePermission struct {
}

func setRemoteToken(clientExtension map[string]string, authenticator []string){
	// Set remote token to clientAuth
	if len(authenticator) > 2 && authenticator[2] != "" {
		clientExtension["token"] = authenticator[2]
	}
}
func (r *RemotePermission) RegisterSubscribePermission(clientExtension map[string]string, authenticator []string, subscribe []string) {
	setRemoteToken(clientExtension, authenticator)
	clientExtension["subscribe"] = subscribe[1]
}


func (r *RemotePermission) RegisterPublishPermission(clientExtension map[string]string, authenticator []string, publish []string) {
	setRemoteToken(clientExtension, authenticator)
	clientExtension["publish"] = publish[1]
}

func (r *RemotePermission) CheckSubscribe(clientExtension map[string]string, subject string) bool {
	if clientExtension["token"] == "" || clientExtension["subscribe"] == "" {
		return false
	}

	token := clientExtension["token"]
	sub := clientExtension["subscribe"]

	values := sub + "?token=" + token + "&&subject=" + subject

	resp, err := http.Get(values)

	if err != nil {
		fmt.Errorf("subAuthenticatorRequest Error: %v", err)
		return false
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Errorf("subAuthenticatorRequest Responese Body Error: %v", err)
		return false
	}

	fmt.Println("subAuthenticatorRequest Body: " + string(body))

	if resp.StatusCode != 200 {
		fmt.Errorf("subAuthenticatorRequest Failed, return %v", resp.StatusCode)
		return false
	}

	return true
}

func (r *RemotePermission) CheckPublish(clientExtension map[string]string, subject string) bool {
	if clientExtension["token"] == "" || clientExtension["publish"] == "" {
		return false
	}

	token := clientExtension["token"]
	pub := clientExtension["publish"]

	values := pub + "?token=" + token + "&&subject=" + subject

	resp, err := http.Get(values)

	if err != nil {
		fmt.Errorf("pubAuthenticatorRequest Error: %v", err)
		return false
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Errorf("pubAuthenticatorRequest Responese Body Error: %v", err)
		return false
	}

	fmt.Println("pubAuthenticatorRequest Body: " + string(body))
	if resp.StatusCode != 200 {
		fmt.Errorf("subAuthenticatorRequest Failed, return %v", resp.StatusCode)
		return false
	}

	return true
}