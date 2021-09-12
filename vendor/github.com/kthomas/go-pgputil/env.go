package pgputil

import (
	"crypto"
	"os"
	"strings"

	"github.com/kthomas/go-logger"
	"golang.org/x/crypto/openpgp"
	"golang.org/x/crypto/openpgp/packet"
)

var (
	log *logger.Logger

	pgpDefaultConfig *packet.Config
	pgpDefaultEntity *openpgp.Entity
	pgpPrivateKey    *packet.PrivateKey
	pgpPublicKey     *packet.PublicKey
	pgpPassphrase    []byte
)

func init() {
	lvl := os.Getenv("PGP_LOG_LEVEL")
	if lvl == "" {
		lvl = "INFO"
	}

	var endpoint *string
	if os.Getenv("SYSLOG_ENDPOINT") != "" {
		endpt := os.Getenv("SYSLOG_ENDPOINT")
		endpoint = &endpt
	}

	log = logger.NewLogger("go-pgputil", lvl, endpoint)
}

// RequirePGP reads `PGP_PUBLIC_KEY`, `PGP_PRIVATE_KEY` and optional
// `PGP_PASSPHRASE` from the environment and panics if the config is invalid.
func RequirePGP() {
	publicKey := strings.Replace(os.Getenv("PGP_PUBLIC_KEY"), `\n`, "\n", -1)
	if publicKey == "" {
		log.Panicf("Failed to resolve PGP_PUBLIC_KEY")
	}
	decodedPublicKey, err := DecodeArmoredPublicKey([]byte(publicKey))
	if err != nil {
		log.Panicf("Failed to resolve PGP_PUBLIC_KEY; %s", err.Error())
	}
	pgpPublicKey = decodedPublicKey

	privateKey := strings.Replace(os.Getenv("PGP_PRIVATE_KEY"), `\n`, "\n", -1)
	if privateKey == "" {
		log.Panicf("Failed to resolve PGP_PRIVATE_KEY")
	}
	decodedPrivateKey, err := DecodeArmoredPrivateKey([]byte(privateKey))
	if err != nil {
		log.Panicf("Failed to resolve PGP_PRIVATE_KEY; %s", err.Error())
	}
	pgpPrivateKey = decodedPrivateKey

	pgpPassphrase = []byte(os.Getenv("PGP_PASSPHRASE"))

	requireDefaultEntity()
}

func requireDefaultEntity() {
	pgpDefaultConfig := &packet.Config{
		DefaultHash:            crypto.SHA256,
		DefaultCipher:          packet.CipherAES256,
		DefaultCompressionAlgo: packet.CompressionZLIB,
		RSABits:                4096,
	}

	currentTime := pgpDefaultConfig.Now()
	uid := packet.NewUserId("", "", "")

	pgpDefaultEntity = &openpgp.Entity{
		PrimaryKey: pgpPublicKey,
		PrivateKey: pgpPrivateKey,
		Identities: make(map[string]*openpgp.Identity),
		// Identities  map[string]*Identity // indexed by Identity.Name
		// Revocations []*packet.Signature
		// Subkeys     []Subkey
	}

	isPrimaryID := true
	pgpDefaultEntity.Identities[uid.Id] = &openpgp.Identity{
		Name:   uid.Name,
		UserId: uid,
		SelfSignature: &packet.Signature{
			CreationTime:  currentTime,
			SigType:       packet.SigTypePositiveCert,
			PubKeyAlgo:    packet.PubKeyAlgoRSA,
			Hash:          pgpDefaultConfig.Hash(),
			PreferredHash: []uint8{8}, // SHA-256
			IsPrimaryId:   &isPrimaryID,
			FlagsValid:    true,
			FlagSign:      true,
			FlagCertify:   true,
			IssuerKeyId:   &pgpDefaultEntity.PrimaryKey.KeyId,
		},
	}

	keyLifetimeSecs := uint32(86400 * 365)

	pgpDefaultEntity.Subkeys = make([]openpgp.Subkey, 1)
	pgpDefaultEntity.Subkeys[0] = openpgp.Subkey{
		PublicKey:  pgpPublicKey,
		PrivateKey: pgpPrivateKey,
		Sig: &packet.Signature{
			CreationTime:              currentTime,
			SigType:                   packet.SigTypeSubkeyBinding,
			PubKeyAlgo:                packet.PubKeyAlgoRSA,
			Hash:                      pgpDefaultConfig.Hash(),
			PreferredHash:             []uint8{8}, // SHA-256
			FlagsValid:                true,
			FlagEncryptStorage:        true,
			FlagEncryptCommunications: true,
			IssuerKeyId:               &pgpDefaultEntity.PrimaryKey.KeyId,
			KeyLifetimeSecs:           &keyLifetimeSecs,
		},
	}
}
