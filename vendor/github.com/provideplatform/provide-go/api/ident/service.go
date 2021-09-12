package ident

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/provideplatform/provide-go/api"
	"github.com/provideplatform/provide-go/common"
)

const defaultIdentHost = "ident.provide.services"
const defaultIdentPath = "api/v1"
const defaultIdentScheme = "https"

// Service for the ident api
type Service struct {
	api.Client
}

// InitDefaultIdentService convenience method to initialize a default `ident.Service` (i.e., production) instance
func InitDefaultIdentService(token *string) *Service {
	return &Service{
		api.Client{
			Host:   defaultIdentHost,
			Path:   defaultIdentPath,
			Scheme: defaultIdentScheme,
			Token:  token,
		},
	}
}

// InitIdentService convenience method to initialize an `ident.Service` instance
func InitIdentService(token *string) *Service {
	host := defaultIdentHost
	if os.Getenv("IDENT_API_HOST") != "" {
		host = os.Getenv("IDENT_API_HOST")
	}

	path := defaultIdentPath
	if os.Getenv("IDENT_API_PATH") != "" {
		path = os.Getenv("IDENT_API_PATH")
	}

	scheme := defaultIdentScheme
	if os.Getenv("IDENT_API_SCHEME") != "" {
		scheme = os.Getenv("IDENT_API_SCHEME")
	}

	return &Service{
		api.Client{
			Host:   host,
			Path:   path,
			Scheme: scheme,
			Token:  token,
		},
	}
}

// Authenticate a user by email address and password, returning a newly-authorized API token
func Authenticate(email, passwd string) (*AuthenticationResponse, error) {
	prvd := InitIdentService(nil)
	status, resp, err := prvd.Post("authenticate", map[string]interface{}{
		"email":    email,
		"password": passwd,
		"scope":    "offline_access",
	})
	if err != nil {
		return nil, err
	}

	// FIXME...
	authresp := &AuthenticationResponse{}
	raw, _ := json.Marshal(resp)
	err = json.Unmarshal(raw, &authresp)

	if err != nil {
		return nil, fmt.Errorf("failed to authenticate user; status: %d; %s", status, err.Error())
	} else if status != 201 {
		return nil, fmt.Errorf("failed to authenticate user; status: %d", status)
	}

	return authresp, nil
}

// CreateApplication on behalf of the given API token
func CreateApplication(token string, params map[string]interface{}) (*Application, error) {
	status, resp, err := InitIdentService(common.StringOrNil(token)).Post("applications", params)
	if err != nil {
		return nil, err
	}

	if status != 201 {
		return nil, fmt.Errorf("failed to create application; status: %v", status)
	}

	// FIXME...
	app := &Application{}
	appraw, _ := json.Marshal(resp)
	err = json.Unmarshal(appraw, &app)

	if err != nil {
		return nil, fmt.Errorf("failed to create application; status: %v; %s", status, err.Error())
	}

	return app, nil
}

// UpdateApplication using the given API token, application id and params
func UpdateApplication(token, applicationID string, params map[string]interface{}) error {
	uri := fmt.Sprintf("applications/%s", applicationID)
	status, _, err := InitIdentService(common.StringOrNil(token)).Put(uri, params)
	if err != nil {
		return err
	}

	if status != 204 {
		return fmt.Errorf("failed to update application; status: %v", status)
	}

	return nil
}

// DeleteApplication soft-deletes the application using the given API token
func DeleteApplication(token, applicationID string) error {
	err := UpdateApplication(token, applicationID, map[string]interface{}{
		"hidden": true,
	})
	if err != nil {
		return err
	}

	return nil
}

// ListApplications retrieves a paginated list of applications scoped to the given API token
func ListApplications(token string, params map[string]interface{}) ([]*Application, error) {
	status, resp, err := InitIdentService(common.StringOrNil(token)).Get("applications", params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to list applications; status: %v", status)
	}

	apps := make([]*Application, 0)
	for _, item := range resp.([]interface{}) {
		app := &Application{}
		appraw, _ := json.Marshal(item)
		json.Unmarshal(appraw, &app)
		apps = append(apps, app)
	}

	return apps, nil
}

// GetApplicationDetails retrives application details for the given API token and application id
func GetApplicationDetails(token, applicationID string, params map[string]interface{}) (*Application, error) {
	uri := fmt.Sprintf("applications/%s", applicationID)
	status, resp, err := InitIdentService(common.StringOrNil(token)).Get(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to fetch application details; status: %v", status)
	}

	// FIXME...
	app := &Application{}
	appraw, _ := json.Marshal(resp)
	err = json.Unmarshal(appraw, &app)

	if err != nil {
		return nil, fmt.Errorf("failed to fetch application details; status: %v; %s", status, err.Error())
	}

	return app, nil
}

// ListApplicationTokens retrieves a paginated list of application API tokens
func ListApplicationTokens(token, applicationID string, params map[string]interface{}) ([]*Token, error) {
	uri := fmt.Sprintf("applications/%s/tokens", applicationID)
	status, resp, err := InitIdentService(common.StringOrNil(token)).Get(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to list application tokens; status: %v", status)
	}

	tkns := make([]*Token, 0)
	for _, item := range resp.([]interface{}) {
		tkn := &Token{}
		tknraw, _ := json.Marshal(item)
		json.Unmarshal(tknraw, &tkn)
		tkns = append(tkns, tkn)
	}

	return tkns, nil
}

// ListApplicationInvitations retrieves a paginated list of invitations scoped to the given API token
func ListApplicationInvitations(token, applicationID string, params map[string]interface{}) ([]*User, error) {
	uri := fmt.Sprintf("applications/%s/invitations", applicationID)
	status, resp, err := InitIdentService(common.StringOrNil(token)).Get(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to list application invitations; status: %v", status)
	}

	users := make([]*User, 0)
	for _, item := range resp.([]interface{}) {
		usr := &User{}
		usrraw, _ := json.Marshal(item)
		json.Unmarshal(usrraw, &usr)
		users = append(users, usr)
	}

	return users, nil
}

// ListApplicationOrganizations retrieves a paginated list of organizations scoped to the given API token
func ListApplicationOrganizations(token, applicationID string, params map[string]interface{}) ([]*Organization, error) {
	uri := fmt.Sprintf("applications/%s/organizations", applicationID)
	status, resp, err := InitIdentService(common.StringOrNil(token)).Get(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to list application organizations; status: %v", status)
	}

	orgs := make([]*Organization, 0)
	for _, item := range resp.([]interface{}) {
		org := &Organization{}
		orgraw, _ := json.Marshal(item)
		json.Unmarshal(orgraw, &org)
		orgs = append(orgs, org)
	}

	return orgs, nil
}

// CreateApplicationOrganization associates an organization with an application
func CreateApplicationOrganization(token, applicationID string, params map[string]interface{}) error {
	uri := fmt.Sprintf("applications/%s/organizations", applicationID)
	status, _, err := InitIdentService(common.StringOrNil(token)).Post(uri, params)
	if err != nil {
		return err
	}

	if status != 204 {
		return fmt.Errorf("failed to associate application organization; status: %v", status)
	}

	return nil
}

// DeleteApplicationOrganization disassociates an organization with an application
func DeleteApplicationOrganization(token, applicationID, organizationID string) error {
	uri := fmt.Sprintf("applications/%s/organizations/%s", applicationID, organizationID)
	status, _, err := InitIdentService(common.StringOrNil(token)).Delete(uri)
	if err != nil {
		return err
	}

	if status != 204 {
		return fmt.Errorf("failed to disassociate application organization; status: %v", status)
	}

	return nil
}

// ListApplicationUsers retrieves a paginated list of users scoped to the given API token
func ListApplicationUsers(token, applicationID string, params map[string]interface{}) ([]*User, error) {
	uri := fmt.Sprintf("applications/%s/users", applicationID)
	status, resp, err := InitIdentService(common.StringOrNil(token)).Get(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to list application users; status: %v", status)
	}

	users := make([]*User, 0)
	for _, item := range resp.([]interface{}) {
		usr := &User{}
		usrraw, _ := json.Marshal(item)
		json.Unmarshal(usrraw, &usr)
		users = append(users, usr)
	}

	return users, nil
}

// CreateApplicationUser associates a user with an application
func CreateApplicationUser(token, applicationID string, params map[string]interface{}) error {
	uri := fmt.Sprintf("applications/%s/users", applicationID)
	status, _, err := InitIdentService(common.StringOrNil(token)).Post(uri, params)
	if err != nil {
		return err
	}

	if status != 204 {
		return fmt.Errorf("failed to associate application user; status: %v", status)
	}

	return nil
}

// DeleteApplicationUser disassociates a user with an application
func DeleteApplicationUser(token, applicationID, userID string) error {
	uri := fmt.Sprintf("applications/%s/users/%s", applicationID, userID)
	status, _, err := InitIdentService(common.StringOrNil(token)).Delete(uri)
	if err != nil {
		return err
	}

	if status != 204 {
		return fmt.Errorf("failed to disassociate application user; status: %v", status)
	}

	return nil
}

// CreateApplicationToken creates a new API token for the given application ID.
func CreateApplicationToken(token, applicationID string, params map[string]interface{}) (*Token, error) {
	params["application_id"] = applicationID
	status, resp, err := InitIdentService(common.StringOrNil(token)).Post("tokens", params)
	if err != nil {
		return nil, err
	}

	// FIXME...
	tkn := &Token{}
	tknraw, _ := json.Marshal(resp)
	err = json.Unmarshal(tknraw, &tkn)

	if err != nil {
		return nil, fmt.Errorf("failed to authorize application token; status: %v; %s", status, err.Error())
	}

	return tkn, nil
}

// ListOrganizations retrieves a paginated list of organizations scoped to the given API token
func ListOrganizations(token string, params map[string]interface{}) ([]*Organization, error) {
	status, resp, err := InitIdentService(common.StringOrNil(token)).Get("organizations", params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to list organizations; status: %v", status)
	}

	orgs := make([]*Organization, 0)
	for _, item := range resp.([]interface{}) {
		org := &Organization{}
		orgraw, _ := json.Marshal(item)
		json.Unmarshal(orgraw, &org)
		orgs = append(orgs, org)
	}

	return orgs, nil
}

// CreateToken creates a new API token.
func CreateToken(token string, params map[string]interface{}) (*Token, error) {
	status, resp, err := InitIdentService(common.StringOrNil(token)).Post("tokens", params)
	if err != nil {
		return nil, err
	}

	if status != 201 {
		return nil, fmt.Errorf("failed to authorize token; status: %v", status)
	}

	// FIXME...
	tkn := &Token{}
	tknraw, _ := json.Marshal(resp)
	err = json.Unmarshal(tknraw, &tkn)

	if err != nil {
		return nil, fmt.Errorf("failed to authorize tokens; status: %v; %s", status, err.Error())
	}

	return tkn, nil
}

// ListTokens retrieves a paginated list of API tokens scoped to the given API token
func ListTokens(token string, params map[string]interface{}) ([]*Token, error) {
	status, resp, err := InitIdentService(common.StringOrNil(token)).Get("tokens", params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to list application tokens; status: %v", status)
	}

	tkns := make([]*Token, 0)
	for _, item := range resp.([]interface{}) {
		tkn := &Token{}
		tknraw, _ := json.Marshal(item)
		json.Unmarshal(tknraw, &tkn)
		tkns = append(tkns, tkn)
	}

	return tkns, nil
}

// GetTokenDetails retrieves details for the given API token id
func GetTokenDetails(token, tokenID string, params map[string]interface{}) (*Token, error) {
	uri := fmt.Sprintf("tokens/%s", tokenID)
	status, resp, err := InitIdentService(common.StringOrNil(token)).Get(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to fetch token details; status: %v", status)
	}

	// FIXME...
	tkn := &Token{}
	tknraw, _ := json.Marshal(resp)
	err = json.Unmarshal(tknraw, &tkn)

	if err != nil {
		return nil, fmt.Errorf("failed to fetch token details; status: %v; %s", status, err.Error())
	}

	return tkn, nil
}

// DeleteToken removes a previously authorized API token, effectively deauthorizing future calls using the token
func DeleteToken(token, tokenID string) error {
	uri := fmt.Sprintf("tokens/%s", tokenID)
	status, _, err := InitIdentService(common.StringOrNil(token)).Delete(uri)
	if err != nil {
		return err
	}

	if status != 204 {
		return fmt.Errorf("failed to revoke token; status: %v", status)
	}

	return nil
}

// CreateOrganization creates a new organization
func CreateOrganization(token string, params map[string]interface{}) (*Organization, error) {
	status, resp, err := InitIdentService(common.StringOrNil(token)).Post("organizations", params)
	if err != nil {
		return nil, err
	}

	if status != 201 {
		return nil, fmt.Errorf("failed to create organization; status: %v", status)
	}

	// FIXME...
	org := &Organization{}
	orgraw, _ := json.Marshal(resp)
	err = json.Unmarshal(orgraw, &org)

	if err != nil {
		return nil, fmt.Errorf("failed to create organization; status: %v; %s", status, err.Error())
	}

	return org, nil
}

// GetOrganizationDetails retrieves details for the given organization
func GetOrganizationDetails(token, organizationID string, params map[string]interface{}) (*Organization, error) {
	uri := fmt.Sprintf("organizations/%s", organizationID)
	status, resp, err := InitIdentService(common.StringOrNil(token)).Get(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to fetch organization; status: %v", status)
	}

	// FIXME...
	org := &Organization{}
	orgraw, _ := json.Marshal(resp)
	err = json.Unmarshal(orgraw, &org)

	if err != nil {
		return nil, fmt.Errorf("failed to fetch organization details; status: %v; %s", status, err.Error())
	}

	return org, nil
}

// UpdateOrganization updates an organization
func UpdateOrganization(token, organizationID string, params map[string]interface{}) error {
	uri := fmt.Sprintf("organizations/%s", organizationID)
	status, _, err := InitIdentService(common.StringOrNil(token)).Put(uri, params)
	if err != nil {
		return err
	}

	if status != 204 {
		return fmt.Errorf("failed to update associated organization user; status: %v", status)
	}

	return nil
}

// CreateInvitation creates a user invitation
func CreateInvitation(token string, params map[string]interface{}) error {
	status, _, err := InitIdentService(common.StringOrNil(token)).Post("invitations", params)
	if err != nil {
		return err
	}

	if status != 204 {
		return fmt.Errorf("failed to create invitation; status: %v", status)
	}

	return nil
}

// CreateUser creates a new user for which API tokens and managed signing identities can be authorized
func CreateUser(token string, params map[string]interface{}) (*User, error) {
	status, resp, err := InitIdentService(common.StringOrNil(token)).Post("users", params)
	if err != nil {
		return nil, err
	}

	// FIXME...
	usr := &User{}
	usrraw, _ := json.Marshal(resp)
	err = json.Unmarshal(usrraw, &usr)

	if err != nil {
		return nil, fmt.Errorf("failed to create user; status: %v; %s", status, err.Error())
	}

	return usr, nil
}

// ListOrganizationUsers retrieves a paginated list of users scoped to an organization
func ListOrganizationUsers(token, orgID string, params map[string]interface{}) ([]*User, error) {
	uri := fmt.Sprintf("organizations/%s/users", orgID)
	status, resp, err := InitIdentService(common.StringOrNil(token)).Get(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to list users; status: %v", status)
	}

	users := make([]*User, 0)
	for _, item := range resp.([]interface{}) {
		usr := &User{}
		usrraw, _ := json.Marshal(item)
		json.Unmarshal(usrraw, &usr)
		users = append(users, usr)
	}

	return users, nil
}

// CreateOrganizationUser associates a user with an organization
func CreateOrganizationUser(token, orgID string, params map[string]interface{}) error {
	uri := fmt.Sprintf("organizations/%s/users", orgID)
	status, _, err := InitIdentService(common.StringOrNil(token)).Post(uri, params)
	if err != nil {
		return err
	}

	if status != 204 {
		return fmt.Errorf("failed to associate organization user; status: %v", status)
	}

	return nil
}

// UpdateOrganizationUser updates an associated organization user=
func UpdateOrganizationUser(token, orgID, userID string, params map[string]interface{}) error {
	uri := fmt.Sprintf("organizations/%s/users/%s", orgID, userID)
	status, _, err := InitIdentService(common.StringOrNil(token)).Put(uri, params)
	if err != nil {
		return err
	}

	if status != 204 {
		return fmt.Errorf("failed to update associated organization user; status: %v", status)
	}

	return nil
}

// DeleteOrganizationUser disassociates a user with an organization
func DeleteOrganizationUser(token, orgID, userID string) error {
	uri := fmt.Sprintf("organizations/%s/users/%s", orgID, userID)
	status, _, err := InitIdentService(common.StringOrNil(token)).Delete(uri)
	if err != nil {
		return err
	}

	if status != 204 {
		return fmt.Errorf("failed to disassociate organization user; status: %v", status)
	}

	return nil
}

// ListOrganizationInvitations retrieves a paginated list of organization invitations scoped to the given API token
func ListOrganizationInvitations(token, organizationID string, params map[string]interface{}) ([]*User, error) {
	uri := fmt.Sprintf("organizations/%s/invitations", organizationID)
	status, resp, err := InitIdentService(common.StringOrNil(token)).Get(uri, params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to list organization invitations; status: %v", status)
	}

	users := make([]*User, 0)
	for _, item := range resp.([]interface{}) {
		usr := &User{}
		usrraw, _ := json.Marshal(item)
		json.Unmarshal(usrraw, &usr)
		users = append(users, usr)
	}

	return users, nil
}

// ListUsers retrieves a paginated list of users scoped to the given API token
func ListUsers(token string, params map[string]interface{}) ([]*User, error) {
	status, resp, err := InitIdentService(common.StringOrNil(token)).Get("users", params)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		return nil, fmt.Errorf("failed to list users; status: %v", status)
	}

	users := make([]*User, 0)
	for _, item := range resp.([]interface{}) {
		usr := &User{}
		usrraw, _ := json.Marshal(item)
		json.Unmarshal(usrraw, &usr)
		users = append(users, usr)
	}

	return users, nil
}

// GetUserDetails retrieves details for the given user id
func GetUserDetails(token, userID string, params map[string]interface{}) (*User, error) {
	uri := fmt.Sprintf("users/%s", userID)
	status, resp, err := InitIdentService(common.StringOrNil(token)).Get(uri, params)
	if err != nil {
		return nil, err
	}

	// FIXME...
	usr := &User{}
	usrraw, _ := json.Marshal(resp)
	err = json.Unmarshal(usrraw, &usr)

	if err != nil {
		return nil, fmt.Errorf("failed to fetch user details; status: %v; %s", status, err.Error())
	}

	return usr, nil
}

// UpdateUser updates an existing user
func UpdateUser(token, userID string, params map[string]interface{}) error {
	uri := fmt.Sprintf("users/%s", userID)
	status, _, err := InitIdentService(common.StringOrNil(token)).Put(uri, params)
	if err != nil {
		return err
	}

	if status != 204 {
		return fmt.Errorf("failed to update user; status: %v", status)
	}

	return nil
}

// RequestPasswordReset initiates a password reset request
func RequestPasswordReset(token, applicationID *string, email string) error {
	params := map[string]interface{}{
		"email": email,
	}
	if applicationID != nil {
		params["application_id"] = applicationID
	}

	status, _, err := InitIdentService(token).Post("users/reset_password", params)
	if err != nil {
		return fmt.Errorf("failed to request password reset; status: %v; %s", status, err.Error())
	}

	if status != 204 {
		return fmt.Errorf("failed to request password reset for user: %s; status: %v", email, status)
	}

	return nil
}

// ResetPassword completes a previously-requested password reset operation for a user
func ResetPassword(token *string, resetPasswordToken, passwd string) error {
	uri := fmt.Sprintf("users/reset_password/%s", resetPasswordToken)
	status, _, err := InitIdentService(token).Post(uri, map[string]interface{}{
		"password": passwd,
	})
	if err != nil {
		return fmt.Errorf("failed to reset password; status: %v; %s", status, err.Error())
	}
	if status != 204 {
		return fmt.Errorf("failed to reset password; status: %v", status)
	}

	return nil
}

// Status returns the status of the endpoint
func Status() error {
	host := defaultIdentHost
	if os.Getenv("IDENT_API_HOST") != "" {
		host = os.Getenv("IDENT_API_HOST")
	}

	scheme := defaultIdentScheme
	if os.Getenv("IDENT_API_SCHEME") != "" {
		scheme = os.Getenv("IDENT_API_SCHEME")
	}

	service := &Service{
		api.Client{
			Host:   host,
			Path:   "",
			Scheme: scheme,
		},
	}

	status, _, err := service.Get("status", map[string]interface{}{})
	if err != nil {
		return fmt.Errorf("failed to fetch status; %s", err.Error())
	}

	if status != 200 {
		return fmt.Errorf("status endpoint returned %d status code", status)
	}

	return nil
}

// GetJWKs returns the set of keys containing the public keys used to verify JWTs
func GetJWKs() ([]*JSONWebKey, error) {
	host := defaultIdentHost
	if os.Getenv("IDENT_API_HOST") != "" {
		host = os.Getenv("IDENT_API_HOST")
	}

	scheme := defaultIdentScheme
	if os.Getenv("IDENT_API_SCHEME") != "" {
		scheme = os.Getenv("IDENT_API_SCHEME")
	}

	service := &Service{
		api.Client{
			Host:   host,
			Path:   "",
			Scheme: scheme,
		},
	}

	status, resp, err := service.Get(".well-known/keys", map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch well-known JWKs; %s", err.Error())
	}

	if status != 200 {
		return nil, fmt.Errorf("well-known JWKs endpoint returned %d status code", status)
	}

	keys := make([]*JSONWebKey, 0)
	for _, item := range resp.([]interface{}) {
		key := &JSONWebKey{}
		keyraw, _ := json.Marshal(item)
		json.Unmarshal(keyraw, &key)
		keys = append(keys, key)
	}

	return keys, nil
}
