package selfsignedcert

import (
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/kthomas/go-logger"
)

const defaultCertificatePath = "./.server.crt"
const defaultPrivateKeyPath = "./.private.key"

const defaultCertificateValidityPeriod = time.Hour * 24 * 365 * 2
const defaultKeySize = 4096

var (
	log *logger.Logger

	certificateOrganization string
	certificatePath         string
	privateKeyPath          string
	keySize                 int

	bootstrapOnce sync.Once
)

func init() {
	bootstrapOnce.Do(func() {
		lvl := os.Getenv("LOG_LEVEL")
		if lvl == "" {
			lvl = "INFO"
		}
		var endpoint *string
		if os.Getenv("SYSLOG_ENDPOINT") != "" {
			endpt := os.Getenv("SYSLOG_ENDPOINT")
			endpoint = &endpt
		}
		log = logger.NewLogger("selfsignedcert", lvl, endpoint)

		if os.Getenv("CERTIFICATE_ORGANIZATION") != "" {
			certificateOrganization = os.Getenv("CERTIFICATE_ORGANIZATION")
		} else {
			hostname, err := os.Hostname()
			if err == nil {
				certificateOrganization = hostname
			}
		}

		if os.Getenv("CERTIFICATE_PATH") != "" {
			certificatePath = os.Getenv("CERTIFICATE_PATH")
		} else {
			certificatePath = defaultCertificatePath
		}

		if os.Getenv("PRIVATE_KEY_PATH") != "" {
			privateKeyPath = os.Getenv("PRIVATE_KEY_PATH")
		} else {
			privateKeyPath = defaultPrivateKeyPath
		}

		if os.Getenv("PRIVATE_KEY_BITS") != "" {
			keySize, _ = strconv.Atoi(os.Getenv("PRIVATE_KEY_BITS"))
		} else {
			keySize = defaultKeySize
		}
	})
}
