package common

import (
	"io/ioutil"
	"net/http"
	"os"
	"time"

	logger "github.com/kthomas/go-logger"
)

const defaultzApplicationClaimsKey = "prvd"
const defaultJWTNatsClaimsKey = "nats"
const defaultJWTAuthorizationAudience = "https://provide.services/api/v1"
const defaultJWTAuthorizationIssuer = "https://ident.provide.services"
const defaultJWTAuthorizationTTL = time.Hour * 24
const defaultNatsJWTAuthorizationAudience = "https://websocket.provide.services"

var (
	// Log is the configured logger
	Log *logger.Logger
)

func init() {
	Log = logger.NewLogger("provide-go", getLogLevel(), getSyslogEndpoint())
}

func getLogLevel() string {
	lvl := os.Getenv("LOG_LEVEL")
	if lvl == "" {
		lvl = "debug"
	}
	return lvl
}

func getSyslogEndpoint() *string {
	var endpoint *string
	if os.Getenv("SYSLOG_ENDPOINT") != "" {
		endpoint = StringOrNil(os.Getenv("SYSLOG_ENDPOINT"))
	}
	return endpoint
}

// ResolvePublicIP resolves the public IP of the caller
func ResolvePublicIP() (*string, error) {
	url := "https://api.ipify.org?format=text" // FIXME
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	ip, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	ipstr := string(ip)
	return &ipstr, nil
}

// StringOrNil returns a pointer to the string, or nil if the given string is empty
func StringOrNil(str string) *string {
	if str == "" {
		return nil
	}
	return &str
}
