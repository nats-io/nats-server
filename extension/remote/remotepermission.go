package remote

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

type RemotePermission struct {
}

func setRemoteToken(clientExtension map[string]string, authenticator []string) {
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

func (r *RemotePermission) CheckSubscribe(clientExtension map[string]string, subject string) error {
	if clientExtension["token"] == "" || clientExtension["subscribe"] == "" {
		return fmt.Errorf("missing token or subscriber checker")
	}

	token := clientExtension["token"]
	sub := clientExtension["subscribe"]

	values := sub + "?token=" + token + "&&subject=" + subject

	resp, err := http.Get(values)

	if err != nil {
		return fmt.Errorf("subAuthenticatorRequest Error: %v", err)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("subAuthenticatorRequest Responese Body Error: %v", err)
	}

	fmt.Println("subAuthenticatorRequest Body: " + string(body))

	if resp.StatusCode != 200 {
		return fmt.Errorf("subAuthenticatorRequest Failed, return %v", resp.StatusCode)
	}

	return nil
}

func (r *RemotePermission) CheckPublish(clientExtension map[string]string, subject string) error {
	if clientExtension["token"] == "" || clientExtension["publish"] == "" {
		return fmt.Errorf("token can not be null")
	}

	token := clientExtension["token"]
	pub := clientExtension["publish"]

	values := pub + "?token=" + token + "&&subject=" + subject

	resp, err := http.Get(values)

	if err != nil {
		return fmt.Errorf("pubAuthenticatorRequest Error: %v", err)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("pubAuthenticatorRequest Responese Body Error: %v", err)
	}

	fmt.Println("pubAuthenticatorRequest Body: " + string(body))
	if resp.StatusCode != 200 {
		return fmt.Errorf("subAuthenticatorRequest Failed, return %v", resp.StatusCode)
	}

	return nil
}
