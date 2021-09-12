package util

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ed25519"
	"golang.org/x/crypto/ssh"

	"github.com/gin-gonic/gin"
	"github.com/kthomas/go-auth0"
	"github.com/kthomas/go-pgputil"

	jwt "github.com/dgrijalva/jwt-go"
	uuid "github.com/kthomas/go.uuid"
	ident "github.com/provideplatform/provide-go/api/ident"
	vault "github.com/provideplatform/provide-go/api/vault"
	common "github.com/provideplatform/provide-go/common"
)

const authorizationHeader = "authorization"

const defaultJWTApplicationClaimsKey = "prvd"
const defaultJWTNatsClaimsKey = "nats"
const defaultJWTAuthorizationAudience = "https://provide.services/api/v1"
const defaultJWTAuthorizationIssuer = "https://ident.provide.services"
const defaultJWTAuthorizationTTL = time.Hour * 24
const defaultNatsJWTAuthorizationAudience = "https://websocket.provide.services"
const defaultTokenSigningKeyspec = "RSA-4096"

const requireJWTSigningKeyTickerInterval = time.Millisecond * 2500
const requireJWTSigningKeySleepInterval = time.Second * 1
const requireJWTSigningKeyTimeout = time.Second * 10

var (
	// defaultJWTKeyFingerprint is the default JWT signing/verifying key fingerprint
	defaultJWTKeyFingerprint *string

	// EdDSA signing method for Ed25519 jwt
	edDSASigningMethod SigningMethodEdDSA

	// JWTApplicationClaimsKey is the key within the JWT payload where application-specific claims are encoded
	JWTApplicationClaimsKey string

	// JWTAuthorizationAudience is the audience who will consume the JWT; this will be set as the JWT "aud" claim
	JWTAuthorizationAudience string

	// JWTAlternativeAuthorizationAudiences are additional valid audiences who will consume signed JWTs, keyed on a scope; these will be allowed to be set as the JWT "aud" claim
	JWTAlternativeAuthorizationAudiences map[string]interface{}

	// JWTAuthorizationIssuer is the common name of the operator of the token vending machine; this will be set as the JWT "iss" claim
	JWTAuthorizationIssuer string

	// JWTAuthorizationTTL is the ttl in milliseconds for new token authorizations, calculated from the issued at timestamp ("iat" claim)
	JWTAuthorizationTTL time.Duration

	// JWTNatsClaimsKey is the key within the JWT claims payload where NATS-specific claims are encoded
	JWTNatsClaimsKey string

	// JWTNatsAuthorizationAudience is the audience who will consume the NATS bearer authorization JWT; this will be set as the JWT "aud" claim
	JWTNatsAuthorizationAudience string

	// Vault is the vault instance
	Vault *vault.Vault

	// JWTKeypairs is a map of JWTKeypair instances which contains the configured RSA public/private keypairs for JWT signing and/or verification,
	// keyed by kid, which in the case of ident-native keypairs, is the fingerprint of the public key
	jwtKeypairs map[string]*JWTKeypair

	jwtVerifiersMutex *sync.Mutex
)

// JWTKeypair enables private key or vault-based JWT signing and verification
type JWTKeypair struct {
	Fingerprint  string
	PublicKey    rsa.PublicKey
	PublicKeyPEM *string
	PrivateKey   *rsa.PrivateKey
	SSHPublicKey *ssh.PublicKey
	VaultKey     *vault.Key
}

// PublicKey returns an associated PublicKey instance.
func (j *JWTKeypair) SSHSigner() ssh.Signer {
	return &JWTKeypairSSHSigner{
		keypair: j,
	}
}

type JWTKeypairSSHSigner struct {
	keypair *JWTKeypair
}

// PublicKey returns an associated PublicKey instance.
func (j *JWTKeypairSSHSigner) PublicKey() ssh.PublicKey {
	if j.keypair != nil && j.keypair.SSHPublicKey != nil {
		return *j.keypair.SSHPublicKey
	}
	return nil
}

// Sign returns raw signature for the given data. This method
// will apply the hash specified for the keytype to the data.
func (j *JWTKeypairSSHSigner) Sign(rand io.Reader, data []byte) (*ssh.Signature, error) {
	return nil, errors.New("not implemented")
}

// SigningMethodEdDSA enables Ed25519
type SigningMethodEdDSA struct{}

// Alg returns EdDSA
func (m *SigningMethodEdDSA) Alg() string {
	return "EdDSA"
}

// Verify the Ed25519 jwt
func (m *SigningMethodEdDSA) Verify(signingString string, signature string, key interface{}) error {
	var err error

	var sig []byte
	if sig, err = jwt.DecodeSegment(signature); err != nil {
		return err
	}

	var ed25519Key ed25519.PublicKey
	var ok bool

	if ed25519Key, ok = key.(ed25519.PublicKey); !ok {
		return jwt.ErrInvalidKeyType
	}

	if len(ed25519Key) != ed25519.PublicKeySize {
		return jwt.ErrInvalidKey
	}

	if ok := ed25519.Verify(ed25519Key, []byte(signingString), sig); !ok {
		return fmt.Errorf("failed to sign %d-byte jwt with Ed25519 key", len(signingString))
	}

	return nil
}

// Sign the jwt using Ed25519
func (m *SigningMethodEdDSA) Sign(signingString string, key interface{}) (str string, err error) {
	var ed25519Key ed25519.PrivateKey
	var ok bool
	if ed25519Key, ok = key.(ed25519.PrivateKey); !ok {
		return "", jwt.ErrInvalidKeyType
	}

	if len(ed25519Key) != ed25519.PrivateKeySize {
		return "", jwt.ErrInvalidKey
	}

	// Sign the string and return the encoded result
	sig := ed25519.Sign(ed25519Key, []byte(signingString))
	return jwt.EncodeSegment(sig), nil
}

func init() {
	jwtPublicKeyPEM := strings.Replace(os.Getenv("JWT_SIGNER_PUBLIC_KEY"), `\n`, "\n", -1)
	if jwtPublicKeyPEM != "" {
		publicKey, err := jwt.ParseRSAPublicKeyFromPEM([]byte(jwtPublicKeyPEM))
		if err != nil {
			common.Log.Panicf("failed to parse JWT public key; %s", err.Error())
		}

		sshPublicKey, err := ssh.NewPublicKey(publicKey)
		if err != nil {
			common.Log.Panicf("failed to resolve JWT public key fingerprint; %s", err.Error())
		}

		defaultJWTKeyFingerprint = common.StringOrNil(ssh.FingerprintLegacyMD5(sshPublicKey))
	}

	jwt.RegisterSigningMethod(edDSASigningMethod.Alg(), func() jwt.SigningMethod { return &edDSASigningMethod })
	jwtVerifiersMutex = &sync.Mutex{}
}

// AuthorizedSubjectID returns the requested JWT subject if it matches
func AuthorizedSubjectID(c *gin.Context, subject string) *uuid.UUID {
	token, err := ParseBearerAuthorizationHeader(c, nil)
	if err != nil {
		common.Log.Tracef("no %s subject claim parsed from bearer authorization header; %s", subject, err.Error())
		return nil
	}
	var id string
	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		if sub, subok := claims["sub"].(string); subok {
			subprts := strings.Split(sub, ":")
			if len(subprts) != 2 {
				common.Log.Debugf("failed to parse %s subject claim from bearer authorization header; subject malformed: %s", subject, sub)
				return nil
			}
			if subprts[0] != subject {
				return nil
			}
			id = subprts[1]
		}
	}
	uuidV4, err := uuid.FromString(id)
	if err != nil {
		common.Log.Debugf("failed to parse %s subject from bearer authorization header; %s", subject, err.Error())
		return nil
	}
	return &uuidV4
}

// ParseBearerAuthorizationHeader parses a bearer authorization header
// expecting to find a valid JWT token; returns the token if present
func ParseBearerAuthorizationHeader(c *gin.Context, keyfunc *func(_jwtToken *jwt.Token) (interface{}, error)) (*jwt.Token, error) {
	authorization := c.GetHeader(authorizationHeader)
	if authorization == "" {
		return nil, errors.New("no authorization header provided")
	}

	hdrprts := strings.Split(authorization, "Bearer ")
	if len(hdrprts) != 2 {
		hdrprts = strings.Split(authorization, "bearer ")
		if len(hdrprts) != 2 {
			return nil, fmt.Errorf("failed to parse bearer authorization header: %s", authorization)
		}
	}

	authorization = hdrprts[1]
	jwtToken, err := jwt.Parse(authorization, func(_jwtToken *jwt.Token) (interface{}, error) {
		if keyfunc != nil {
			fn := *keyfunc
			return fn(_jwtToken)
		}

		_, rsaSigningMethodOk := _jwtToken.Method.(*jwt.SigningMethodRSA)
		_, eddsaSigningMethodOk := _jwtToken.Method.(*SigningMethodEdDSA)
		if !rsaSigningMethodOk && !eddsaSigningMethodOk {
			return nil, fmt.Errorf("failed to parse bearer authorization header; unexpected JWT signing alg: %s", _jwtToken.Method.Alg())
		}

		var kid *string
		if kidhdr, ok := _jwtToken.Header["kid"].(string); ok {
			kid = &kidhdr
		}

		publicKey, _, _, _ := ResolveJWTKeypair(kid)
		if publicKey == nil {
			msg := "failed to resolve a valid JWT verification key"
			if kid != nil {
				msg = fmt.Sprintf("%s; invalid kid specified in header: %s", msg, *kid)
			} else {
				msg = fmt.Sprintf("%s; no default verification key configured", msg)
			}
			return nil, fmt.Errorf(msg)
		}

		return publicKey, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to parse bearer authentication header as valid JWT; %s", err.Error())
	}
	return jwtToken, err
}

// RequireJWT allows a package to conditionally require a valid JWT configuration
// in the ident environment; at least one RS256 keypair must be configured using
// the JWT_SIGNER_PRIVATE_KEY and JWT_SIGNER_PUBLIC_KEY environment variables
func RequireJWT() map[string]*JWTKeypair {
	common.Log.Debug("attempting to read required JWT configuration environment for signing JWT tokens")

	if os.Getenv("JWT_APPLICATION_CLAIMS_KEY") != "" {
		JWTApplicationClaimsKey = os.Getenv("JWT_APPLICATION_CLAIMS_KEY")
	} else {
		JWTApplicationClaimsKey = defaultJWTApplicationClaimsKey
	}

	if os.Getenv("JWT_NATS_CLAIMS_KEY") != "" {
		JWTNatsClaimsKey = os.Getenv("JWT_NATS_CLAIMS_KEY")
	} else {
		JWTNatsClaimsKey = defaultJWTNatsClaimsKey
	}

	JWTNatsAuthorizationAudience = os.Getenv("JWT_NATS_AUTHORIZATION_AUDIENCE")
	if JWTNatsAuthorizationAudience == "" {
		JWTNatsAuthorizationAudience = defaultNatsJWTAuthorizationAudience
	}

	JWTAuthorizationAudience = os.Getenv("JWT_AUTHORIZATION_AUDIENCE")
	if JWTAuthorizationAudience == "" {
		JWTAuthorizationAudience = defaultJWTAuthorizationAudience
	} else if JWTAuthorizationAudience == "_self" {
		ip, err := common.ResolvePublicIP()
		if err != nil {
			common.Log.Panicf("failed to resolve public ip; %s", err.Error())
		}
		JWTAuthorizationAudience = fmt.Sprintf("http://%s:%s/api/v1", *ip, ListenPort)
	}

	JWTAlternativeAuthorizationAudiences = map[string]interface{}{}
	if os.Getenv("JWT_ALT_AUTHORIZATION_AUDIENCES") != "" {
		err := json.Unmarshal([]byte(os.Getenv("JWT_ALT_AUTHORIZATION_AUDIENCES")), &JWTAlternativeAuthorizationAudiences)
		if err != nil {
			common.Log.Panicf("failed to parse JWT_ALT_AUTHORIZATION_AUDIENCES from environment; %s", err.Error())
		}
	}

	JWTAuthorizationIssuer = os.Getenv("JWT_AUTHORIZATION_ISSUER")
	if JWTAuthorizationIssuer == "" {
		JWTAuthorizationIssuer = defaultJWTAuthorizationIssuer
	}

	if os.Getenv("JWT_AUTHORIZATION_TTL") != "" {
		ttlMillis, err := strconv.Atoi(os.Getenv("JWT_AUTHORIZATION_TTL"))
		if err != nil {
			common.Log.Panicf("failed to parse JWT_AUTHORIZATION_TTL from environment; %s", err.Error())
		}
		JWTAuthorizationTTL = time.Millisecond * time.Duration(ttlMillis)
	} else {
		JWTAuthorizationTTL = defaultJWTAuthorizationTTL
	}

	return requireJWTKeypairs()
}

// RequireJWTVerifiers allows a package to conditionally require RS256 signature
// verification in the configured environment via JWT_SIGNER_PUBLIC_KEY; the
// use-case for this support is when another microservice is depending on the
// token authorization middleware provided in this package
func RequireJWTVerifiers() map[string]*JWTKeypair {
	jwtVerifiersMutex.Lock()
	defer jwtVerifiersMutex.Unlock()

	common.Log.Debug("attempting to read required public key from environment for verifying signed JWT")
	if jwtKeypairs == nil {
		jwtKeypairs = map[string]*JWTKeypair{}
	}

	jwtPublicKeyPEM := strings.Replace(os.Getenv("JWT_SIGNER_PUBLIC_KEY"), `\n`, "\n", -1)
	publicKey, err := pgputil.DecodeRSAPublicKeyFromPEM([]byte(jwtPublicKeyPEM))
	if err != nil {
		common.Log.Panicf("failed to parse JWT public key; %s", err.Error())
	}

	sshPublicKey, err := ssh.NewPublicKey(publicKey)
	if err != nil {
		common.Log.Panicf("failed to resolve JWT public key fingerprint; %s", err.Error())
	}
	fingerprint := ssh.FingerprintLegacyMD5(sshPublicKey)

	jwtKeypairs[fingerprint] = &JWTKeypair{
		Fingerprint:  fingerprint,
		PublicKey:    *publicKey,
		PublicKeyPEM: &jwtPublicKeyPEM,
		SSHPublicKey: &sshPublicKey,
	}

	defaultJWTKeyFingerprint = &fingerprint
	common.Log.Debugf("JWT_SIGNER_PUBLIC_KEY keypair configured: %s", fingerprint)

	requireIdentJWTVerifiers()
	requireAuth0JWTVerifiers()

	return jwtKeypairs
}

func requireAuth0JWTVerifiers() {
	if os.Getenv("AUTH0_DOMAIN") != "" {
		keys, err := auth0.GetJWKs()
		if err != nil {
			common.Log.Warningf("failed to resolve auth0 jwt keys; %s", err.Error())
		} else {
			for kid := range keys {
				publicKey := keys[kid].(rsa.PublicKey)
				sshPublicKey, err := ssh.NewPublicKey(&publicKey)
				if err != nil {
					common.Log.Panicf("failed to resolve JWT public key fingerprint; %s", err.Error())
				}
				fingerprint := ssh.FingerprintLegacyMD5(sshPublicKey)

				encodedPublicKeyBytes, _ := x509.MarshalPKIXPublicKey(&publicKey)
				pkixPublicKey := pem.EncodeToMemory(&pem.Block{
					Type:  "PUBLIC KEY",
					Bytes: encodedPublicKeyBytes,
				})

				// auth0 keys are index by kid, not fingerprint
				jwtKeypairs[kid] = &JWTKeypair{
					Fingerprint:  fingerprint,
					PublicKey:    publicKey,
					PublicKeyPEM: common.StringOrNil(string(pkixPublicKey)),
					SSHPublicKey: &sshPublicKey,
				}

				common.Log.Debugf("auth0 jwt public key configured for verification; kid: %s; fingerprint: %s", kid, fingerprint)
			}
		}
	}
}

func requireIdentJWTVerifiers() {
	keys, err := ident.GetJWKs()
	if err != nil {
		common.Log.Warningf("failed to resolve ident jwt keys; %s", err.Error())
	} else {
		for _, key := range keys {
			publicKey, err := pgputil.DecodeRSAPublicKeyFromPEM([]byte(key.PublicKey))
			if err != nil {
				common.Log.Warningf("failed to parse ident JWT public key; %s", err.Error())
			}

			sshPublicKey, err := ssh.NewPublicKey(publicKey)
			if err != nil {
				common.Log.Warningf("failed to resolve JWT public key fingerprint; %s", err.Error())
			}
			fingerprint := ssh.FingerprintLegacyMD5(sshPublicKey)

			new := jwtKeypairs[fingerprint] == nil

			jwtKeypairs[fingerprint] = &JWTKeypair{
				Fingerprint:  fingerprint,
				PublicKey:    *publicKey,
				PublicKeyPEM: &key.PublicKey,
				SSHPublicKey: &sshPublicKey,
			}

			if new {
				common.Log.Debugf("ident jwt public key configured for verification; fingerprint: %s", fingerprint)
			} else {
				common.Log.Tracef("ident jwt public key configured for verification; fingerprint: %s", fingerprint)
			}
		}
	}
}

// ResolveJWTKeypair returns the configured public/private signing keypair and its
// fingerprint, if one has been configured; this impl will be upgraded soon to allow
// many key to be configured
func ResolveJWTKeypair(fingerprint *string) (*rsa.PublicKey, *rsa.PrivateKey, *vault.Key, *string) {
	if jwtKeypairs == nil || len(jwtKeypairs) == 0 {
		return nil, nil, nil, nil
	}

	var keypair *JWTKeypair

	if fingerprint == nil {
		fingerprints := resolveJWTFingerprints()
		if defaultJWTKeyFingerprint != nil {
			keypair = jwtKeypairs[*defaultJWTKeyFingerprint]
		} else {
			keypair = jwtKeypairs[fingerprints[len(fingerprints)-1]]
		}
	} else {
		if jwtKeypair, jwtKeypairOk := jwtKeypairs[*fingerprint]; jwtKeypairOk {
			keypair = jwtKeypair
		}
	}

	if keypair == nil {
		return nil, nil, nil, nil
	}

	return &keypair.PublicKey, keypair.PrivateKey, keypair.VaultKey, &keypair.Fingerprint
}

func requireJWTKeypairs() map[string]*JWTKeypair {
	common.Log.Debug("attempting to read required RS256 keypair(s) from environment for signing JWT tokens")
	if jwtKeypairs == nil {
		jwtKeypairs = map[string]*JWTKeypair{}
	}

	var privateKey *rsa.PrivateKey // deprecated... only use the vault for signing...
	var err error

	if os.Getenv("JWT_SIGNER_PRIVATE_KEY") != "" {
		jwtPrivateKeyPEM := strings.Replace(os.Getenv("JWT_SIGNER_PRIVATE_KEY"), `\n`, "\n", -1)
		privateKey, err = pgputil.DecodeRSAPrivateKeyFromPEM([]byte(jwtPrivateKeyPEM))
		if err != nil {
			common.Log.Panicf("failed to parse JWT private key; %s", err.Error())
		}
	}

	if os.Getenv("JWT_SIGNER_PUBLIC_KEY") != "" {
		jwtPublicKeyPEM := strings.Replace(os.Getenv("JWT_SIGNER_PUBLIC_KEY"), `\n`, "\n", -1)
		publicKey, err := pgputil.DecodeRSAPublicKeyFromPEM([]byte(jwtPublicKeyPEM))
		if err != nil {
			common.Log.Panicf("failed to parse JWT public key; %s", err.Error())
		}

		sshPublicKey, err := ssh.NewPublicKey(publicKey)
		if err != nil {
			common.Log.Panicf("failed to resolve JWT public key fingerprint; %s", err.Error())
		}
		fingerprint := ssh.FingerprintLegacyMD5(sshPublicKey)

		jwtKeypairs[fingerprint] = &JWTKeypair{
			Fingerprint:  fingerprint,
			PublicKey:    *publicKey,
			PublicKeyPEM: &jwtPublicKeyPEM,
			PrivateKey:   privateKey,
			SSHPublicKey: &sshPublicKey,
		}

		defaultJWTKeyFingerprint = &fingerprint
		common.Log.Debugf("resolved JWT signing key from environment: %s", fingerprint)
	}

	go requireVaultJWTKeypairs()
	requireAuth0JWTVerifiers()

	return jwtKeypairs
}

func resolveJWTFingerprints() []string {
	fingerprints := make([]string, 0, len(jwtKeypairs))
	for k := range jwtKeypairs {
		fingerprints = append(fingerprints, k)
	}
	return fingerprints
}

func requireVaultJWTKeypairs() {
	RequireVault()

	startTime := time.Now()

	timer := time.NewTicker(requireJWTSigningKeyTickerInterval)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			if Vault != nil {
				keys, err := vault.ListKeys(DefaultVaultAccessJWT, Vault.ID.String(), map[string]interface{}{
					"spec": defaultTokenSigningKeyspec,
				})
				if err != nil {
					common.Log.Warningf("failed to fetch keys for vault: %s; %s", Vault.ID.String(), err.Error())
					continue
				}

				var jwtSigningKey *vault.Key

				if len(keys) > 0 {
					jwtSigningKey = keys[0]
				} else {
					jwtSigningKey, err = vault.CreateKey(DefaultVaultAccessJWT, Vault.ID.String(), map[string]interface{}{
						"name":        fmt.Sprintf("JWT %s signer", defaultTokenSigningKeyspec),
						"description": fmt.Sprintf("JWT %s signer", defaultTokenSigningKeyspec),
						"spec":        defaultTokenSigningKeyspec,
						"type":        "asymmetric",
						"usage":       "sign/verify",
					})
					if err != nil {
						common.Log.Warningf("failed to create default JWT signing key; %s", err.Error())
						continue
					}

					common.Log.Debugf("created default JWT signing key: %s", jwtSigningKey.ID.String())
					common.Log.Tracef("JWT signing key public key: %s", *jwtSigningKey.PublicKey)
				}

				publicKey, err := pgputil.DecodeRSAPublicKeyFromPEM([]byte(*jwtSigningKey.PublicKey))
				if err != nil {
					common.Log.Warningf("failed to parse JWT public key; %s", err.Error())
					continue
				}

				sshPublicKey, err := ssh.NewPublicKey(publicKey)
				if err != nil {
					common.Log.Warningf("failed to resolve JWT public key fingerprint; %s", err.Error())
					continue
				}

				fingerprint := common.StringOrNil(ssh.FingerprintLegacyMD5(sshPublicKey))

				jwtKeypairs[*fingerprint] = &JWTKeypair{
					Fingerprint:  *fingerprint,
					PublicKey:    *publicKey,
					PublicKeyPEM: jwtSigningKey.PublicKey,
					SSHPublicKey: &sshPublicKey,
					VaultKey:     jwtSigningKey,
				}

				defaultJWTKeyFingerprint = fingerprint
				common.Log.Debugf("resolved JWT signing key from vault: %s", *defaultJWTKeyFingerprint)
				return
			}
		default:
			if startTime.Add(requireJWTSigningKeyTimeout).Before(time.Now()) {
				common.Log.Panicf("failed to require JWT signing key")
			} else {
				time.Sleep(requireJWTSigningKeySleepInterval)
			}
		}
	}
}
