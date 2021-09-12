package util

import (
	"fmt"
	"time"

	"github.com/provideplatform/provide-go/api"
	common "github.com/provideplatform/provide-go/common"
)

const maxManifestRetryCount = 10

// ResolveCapabilitiesManifest attempts to resolve the capabilities manifest from S3
func ResolveCapabilitiesManifest() (map[string]interface{}, error) {
	i := 0
	for {
		common.Log.Tracef("attempting to fetch capabilities manifest (attempt #%d)", i)
		client := &api.Client{
			Host:   "s3.amazonaws.com",
			Scheme: "https",
			Path:   "static.provide.services/capabilities",
		}

		_, resp, err := client.Get("provide-capabilities-manifest.json", map[string]interface{}{})
		if err != nil {
			common.Log.Warningf("failed to fetch capabilities manifest; %s", err.Error())
		} else if capabilities, ok := resp.(map[string]interface{}); ok {
			return capabilities, nil
		}

		i++
		if i == maxManifestRetryCount {
			return nil, fmt.Errorf("failed to retrieve capabilities manifest after %d attempts", i)
		}
		time.Sleep(time.Millisecond * 25)
	}
}
