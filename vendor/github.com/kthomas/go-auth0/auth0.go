package auth0

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// AuthenticateUser authenticates an auth0 user
func AuthenticateUser(email, password string) (interface{}, error) {
	client, err := NewAuth0APIClientWithPath("oauth")
	if err != nil {
		log.Warningf("failed to create auth0 user; %s", err.Error())
		return nil, err
	}

	status, resp, err := client.Post("ro", map[string]interface{}{
		"client_id":  os.Getenv("AUTH0_CLIENT_ID"),
		"username":   email,
		"password":   password,
		"connection": "Username-Password-Authentication",
		"scope":      "openid",
	})
	if err != nil {
		log.Warningf("failed to authenticate auth0 user; %s", err.Error())
		return nil, err
	}
	if status != 201 {
		msg := fmt.Sprintf("failed to authenticate auth0 user; status code: %d; resp: %s", status, resp)
		log.Warning(msg)
		return nil, errors.New(msg)
	}

	return resp, nil
}

// GetUser returns an auth0 user by id
func GetUser(auth0UserID string) (interface{}, error) {
	client, err := NewAuth0APIClient()
	if err != nil {
		log.Warningf("failed to fetch auth0 user; %s", err.Error())
		return nil, err
	}

	status, resp, err := client.Get(fmt.Sprintf("users/%s", auth0UserID), nil)
	if err != nil {
		log.Warningf("failed to fetch auth0 user; %s", err.Error())
		return nil, err
	}
	if status != 200 {
		msg := fmt.Sprintf("failed to fetch auth0 user; status code: %d", status)
		log.Warning(msg)
		return nil, errors.New(msg)
	}

	return resp, nil
}

// GetUserByEmail returns an auth0 user by email address
func GetUserByEmail(email string) (interface{}, error) {
	client, err := NewAuth0APIClient()
	if err != nil {
		log.Warningf("failed to fetch auth0 user by email: %s; %s", email, err.Error())
		return nil, err
	}

	status, resp, err := client.Get("users-by-email", map[string]interface{}{
		"email": email,
	})
	if err != nil {
		log.Warningf("failed to fetch auth0 user by email: %s; %s", email, err.Error())
		return nil, err
	}
	if status != 200 {
		msg := fmt.Sprintf("failed to fetch auth0 user by email: %s; status code: %d", email, status)
		log.Warning(msg)
		return nil, errors.New(msg)
	}

	return resp, nil
}

// CreateUser creates an auth0 user
func CreateUser(params map[string]interface{}) (interface{}, error) {
	client, err := NewAuth0APIClient()
	if err != nil {
		log.Warningf("failed to create auth0 user; %s", err.Error())
		return nil, err
	}

	status, resp, err := client.Post("users", params)
	if err != nil {
		log.Warningf("failed to create auth0 user; %s", err.Error())
		return nil, err
	}
	if status != 201 {
		msg := fmt.Sprintf("failed to create auth0 user; status code: %d; resp: %s", status, resp)
		log.Warning(msg)
		return nil, errors.New(msg)
	}

	return resp, nil
}

// UpdateUser updates an auth0 user
func UpdateUser(auth0UserID string, params map[string]interface{}) (interface{}, error) {
	client, err := NewAuth0APIClient()
	if err != nil {
		log.Warningf("failed to update auth0 user; %s", err.Error())
		return nil, err
	}

	status, resp, err := client.Patch(fmt.Sprintf("users/%s", auth0UserID), params)
	if err != nil {
		log.Warningf("failed to update auth0 user; %s", err.Error())
		return nil, err
	}
	if status != 200 {
		msg := fmt.Sprintf("failed to update auth0 user; status code: %d; resp: %s", status, resp)
		log.Warning(msg)
		return nil, errors.New(msg)
	}

	return resp, nil
}

// DeleteUser deletes an auth0 user
func DeleteUser(auth0UserID string) (interface{}, error) {
	client, err := NewAuth0APIClient()
	if err != nil {
		log.Warningf("failed to delete auth0 user; %s", err.Error())
		return nil, err
	}

	status, resp, err := client.Delete(fmt.Sprintf("users/%s", auth0UserID))
	if err != nil {
		log.Warningf("failed to delete auth0 user; %s", err.Error())
		return nil, err
	}
	if status != 204 {
		msg := fmt.Sprintf("failed to delete auth0 user; status code: %d", status)
		log.Warning(msg)
		return nil, errors.New(msg)
	}

	return resp, nil
}

// ExportUsers returns an export of all auth0 users
func ExportUsers() ([]interface{}, error) {
	client, err := NewAuth0APIClient()
	if err != nil {
		log.Warningf("failed to export auth0 users; %s", err.Error())
		return nil, err
	}

	status, resp, err := client.Post("jobs/users-exports", map[string]interface{}{
		"fields": []map[string]string{
			map[string]string{"name": "user_id"},
			map[string]string{"name": "name"},
			map[string]string{"name": "email"},
			map[string]string{"name": "created_at"},
			map[string]string{"name": "app_metadata"},
		},
		"format": "json",
	})
	if err != nil {
		log.Warningf("failed to export auth0 users; %s", err.Error())
		return nil, err
	}
	if status != 200 {
		msg := fmt.Sprintf("failed to export auth0 users; status: %d; response: %s", status, resp)
		log.Warning(msg)
		return nil, errors.New(msg)
	}

	users := make([]interface{}, 0)
	auth0JobID := resp.(map[string]interface{})["id"].(string)
	for {
		job, err := GetJob(auth0JobID)
		if err != nil {
			log.Warningf("failed to fetch auth0 export users job; %s", err.Error())
		}
		if job != nil {
			if status, statusOk := job.(map[string]interface{})["status"].(string); statusOk {
				if status == "completed" {
					usersExportURL := job.(map[string]interface{})["location"].(string)
					status, resp, err := client.sendRequest("GET", usersExportURL, "", nil)
					if err != nil {
						log.Warningf("failed to fetch compressed auth0 export users artifact from location: %s; %s", usersExportURL, err.Error())
					} else if status == 200 {
						if decompressedUsersExport, decompressedUsersExportOk := resp.([]byte); decompressedUsersExportOk {
							log.Debugf("exported users:\n\n%s\n\n", string(decompressedUsersExport))
							lines := strings.Split(string(decompressedUsersExport), "\n")
							for i := range lines {
								if len(lines[i]) == 0 {
									continue
								}
								var usr map[string]interface{}
								err := json.Unmarshal([]byte(lines[i]), &usr)
								if err != nil {
									log.Warningf("failed to unmarshal auth0 user from exported users artifact on line %d; %s", i, err.Error())
								} else {
									users = append(users, usr)
								}
							}
						}
						break
					}
				}
			}
		}

		time.Sleep(time.Millisecond * 2500)
	}

	return users, nil
}

// GetJob returns an auth0 job by id
func GetJob(auth0JobID string) (interface{}, error) {
	client, err := NewAuth0APIClient()
	if err != nil {
		log.Warningf("failed to fetch auth0 user; %s", err.Error())
		return nil, err
	}

	status, resp, err := client.Get(fmt.Sprintf("jobs/%s", auth0JobID), nil)
	if err != nil {
		log.Warningf("failed to fetch auth0 job; %s", err.Error())
		return nil, err
	}
	if status != 200 {
		msg := fmt.Sprintf("failed to fetch auth0 job; %s", err.Error())
		log.Warning(msg)
		return nil, errors.New(msg)
	}

	return resp, nil
}
