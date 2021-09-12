package ident

import (
	"time"

	uuid "github.com/kthomas/go.uuid"

	"github.com/provideplatform/provide-go/api"
)

// Application model which is initially owned by the user who created it
type Application struct {
	api.Model

	NetworkID   uuid.UUID              `json:"network_id,omitempty"`
	UserID      uuid.UUID              `json:"user_id,omitempty"` // this is the user that initially created the app
	Name        *string                `json:"name"`
	Description *string                `json:"description"`
	Status      *string                `json:"status,omitempty"` // this is for enrichment purposes only
	Type        *string                `json:"type"`
	Config      map[string]interface{} `json:"config"`
	Hidden      bool                   `json:"hidden"`
}

// AuthenticationResponse is returned upon successful authentication of a user (i.e., by email address)
type AuthenticationResponse struct {
	User  *User  `json:"user"`
	Token *Token `json:"token"`
}

// Invite model
type Invite struct {
	api.Model

	ApplicationID    *uuid.UUID             `json:"application_id,omitempty"`
	UserID           *uuid.UUID             `json:"user_id,omitempty"`
	FirstName        *string                `json:"first_name,omitempty"`
	LastName         *string                `json:"last_name,omitempty"`
	Email            *string                `json:"email,omitempty"`
	InvitorID        *uuid.UUID             `json:"invitor_id,omitempty"`
	InvitorName      *string                `json:"invitor_name,omitempty"`
	OrganizationID   *uuid.UUID             `json:"organization_id,omitempty"`
	OrganizationName *string                `json:"organization_name,omitempty"`
	Permissions      uint32                 `json:"permissions,omitempty"`
	Params           map[string]interface{} `json:"params,omitempty"`
}

// JSONWebKey represents the public part of a JWT
type JSONWebKey struct {
	Kid string   `json:"kid,omitempty"`
	Use string   `json:"use,omitempty"`
	N   string   `json:"n,omitempty"`
	E   string   `json:"e,omitempty"`
	X5c []string `json:"x5c,omitempty"`

	Fingerprint string `json:"fingerprint,omitempty"`
	PublicKey   string `json:"public_key,omitempty"`
}

// Organization model
type Organization struct {
	api.Model

	Name        *string                `json:"name"`
	UserID      *uuid.UUID             `json:"user_id,omitempty"`
	Description *string                `json:"description"`
	Permissions uint32                 `json:"permissions,omitempty"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// Token represents a bearer JWT
type Token struct {
	api.Model

	Token *string `json:"token,omitempty"`

	// OAuth 2 fields
	AccessToken  *string `json:"access_token,omitempty"`
	RefreshToken *string `json:"refresh_token,omitempty"`
	Scope        *string `json:"scope,omitempty"`

	// Ephemeral JWT header fields and claims; these are here for convenience and
	// are not always populated, even if they exist on the underlying token
	Kid       *string    `json:"kid,omitempty"` // key fingerprint
	Audience  *string    `json:"audience,omitempty"`
	Issuer    *string    `json:"issuer,omitempty"`
	IssuedAt  *time.Time `json:"issued_at,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	ExpiresIn *uint64    `json:"expires_in,omitempty"`
	NotBefore *time.Time `json:"not_before_at,omitempty"`
	Subject   *string    `json:"subject,omitempty"`

	Permissions uint32                 `json:"permissions,omitempty"`
	Data        map[string]interface{} `json:"data,omitempty"`
}

// User represents a user
type User struct {
	api.Model

	Name                   string                 `json:"name"`
	FirstName              string                 `json:"first_name"`
	LastName               string                 `json:"last_name"`
	Email                  string                 `json:"email"`
	Permissions            uint32                 `json:"permissions,omitempty,omitempty"`
	PrivacyPolicyAgreedAt  *time.Time             `json:"privacy_policy_agreed_at,omitempty"`
	TermsOfServiceAgreedAt *time.Time             `json:"terms_of_service_agreed_at,omitempty"`
	Metadata               map[string]interface{} `json:"metadata,omitempty"`
}
