// Copyright 2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package certidp

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/ocsp"
)

func FetchOCSPResponse(link *ChainLink, opts *OCSPPeerConfig, log *Log) ([]byte, error) {
	if link == nil || link.Leaf == nil || link.Issuer == nil || opts == nil || log == nil {
		return nil, fmt.Errorf(ErrInvalidChainlink)
	}

	timeout := time.Duration(opts.Timeout * float64(time.Second))
	if timeout <= 0*time.Second {
		timeout = DefaultOCSPResponderTimeout
	}

	getRequestBytes := func(u string, hc *http.Client) ([]byte, error) {
		resp, err := hc.Get(u)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf(ErrBadResponderHTTPStatus, resp.StatusCode)
		}
		return io.ReadAll(resp.Body)
	}

	// Request documentation:
	// https://tools.ietf.org/html/rfc6960#appendix-A.1

	reqDER, err := ocsp.CreateRequest(link.Leaf, link.Issuer, nil)
	if err != nil {
		return nil, err
	}

	reqEnc := base64.StdEncoding.EncodeToString(reqDER)

	responders := *link.OCSPWebEndpoints

	if len(responders) == 0 {
		return nil, fmt.Errorf(ErrNoAvailOCSPServers)
	}

	var raw []byte
	hc := &http.Client{
		Timeout: timeout,
	}
	for _, u := range responders {
		url := u.String()
		log.Debugf(DbgMakingCARequest, url)
		url = strings.TrimSuffix(url, "/")
		raw, err = getRequestBytes(fmt.Sprintf("%s/%s", url, reqEnc), hc)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf(ErrFailedWithAllRequests, err)
	}

	return raw, nil
}
