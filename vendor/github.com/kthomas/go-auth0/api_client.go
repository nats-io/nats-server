package auth0

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/vincent-petithory/dataurl"
)

const defaultContentType = "application/json"
const defaultRequestTimeout = time.Second * 10

// Auth0APIClient is a generic base class for calling a REST API; when a token is configured on an
// Auth0APIClient instance it will be provided as a bearer authorization header; when a username and
// password are configured on an Auth0APIClient instance, they will be used for HTTP basic authorization
// but will be passed as the Authorization header instead of as part of the URL itself. When a token
// is confgiured on an Auth0APIClient instance, the username and password supplied for basic auth are
// currently discarded.
type Auth0APIClient struct {
	Host                string
	OAuthAccessTokenURL string
	Path                string
	Scheme              string
	Token               *string
	TokenExpiresAt      *time.Time
	Username            *string
	Password            *string
}

// NewAuth0APIClient initializes a Auth0APIClient to interact with the Auth0 API using the
// environment-configured Auth0 API credentials.
func NewAuth0APIClient() (*Auth0APIClient, error) {
	return NewAuth0APIClientWithPath(fmt.Sprintf("api/%s", auth0APINamespace))
}

// NewAuth0APIClientWithPath initializes a Auth0APIClient to interact with the Auth0 API using the
// environment-configured Auth0 API credentials and given path.
func NewAuth0APIClientWithPath(path string) (*Auth0APIClient, error) {
	domain := auth0Domain
	if !strings.HasPrefix(domain, "https://") {
		domain = fmt.Sprintf("https://%s", domain)
	}
	apiURL, err := url.Parse(domain)
	if err != nil {
		log.Warningf("Failed to parse auth0 API base url; %s", err.Error())
		return nil, err
	}

	return &Auth0APIClient{
		Host:                apiURL.Host,
		Scheme:              apiURL.Scheme,
		Path:                path,
		OAuthAccessTokenURL: fmt.Sprintf("%s/oauth/token", domain),
	}, nil
}

func (c *Auth0APIClient) getAccessToken() error {
	issuedAt := time.Now()
	status, resp, err := c.sendRequest("POST", c.OAuthAccessTokenURL, "application/json", map[string]interface{}{
		"audience":      auth0Audience,
		"client_id":     auth0ClientID,
		"client_secret": auth0ClientSecret,
		"grant_type":    "client_credentials",
	})
	if err != nil {
		log.Warningf("Failed to parse auth0 API base url; %s", err.Error())
		return err
	}
	if status == 200 {
		if accessToken, accessTokenOk := resp.(map[string]interface{})["access_token"].(string); accessTokenOk {
			c.Token = &accessToken

			if accessTokenTTL, accessTokenTTLOk := resp.(map[string]interface{})["expires_in"].(float64); accessTokenTTLOk {
				expiresAt := issuedAt.Add(time.Second * time.Duration(accessTokenTTL))
				c.TokenExpiresAt = &expiresAt
			}
		}
	}
	return nil
}

func (c *Auth0APIClient) sendRequest(method, urlString, contentType string, params map[string]interface{}) (status int, response interface{}, err error) {
	return c.sendRequestWithTLSClientConfig(method, urlString, contentType, params,
		&tls.Config{
			InsecureSkipVerify: false,
		},
	)
}

func (c *Auth0APIClient) sendRequestWithTLSClientConfig(method, urlString, contentType string, params map[string]interface{}, tlsClientConfig *tls.Config) (status int, response interface{}, err error) {
	if urlString != c.OAuthAccessTokenURL && c.Token == nil || (c.Token != nil && c.TokenExpiresAt != nil && c.TokenExpiresAt.Before(time.Now())) {
		c.Token = nil
		c.TokenExpiresAt = nil

		err := c.getAccessToken()
		if err != nil {
			log.Warningf("failed to refresh access token; %s", err.Error())
			return -1, nil, err
		}
	}
	client := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
			TLSClientConfig:   tlsClientConfig,
		},
		Timeout: defaultRequestTimeout,
	}

	mthd := strings.ToUpper(method)
	reqURL, err := url.Parse(urlString)
	if err != nil {
		log.Warningf("Failed to parse URL for HTTP %s request; URL: %s; %s", method, urlString, err.Error())
		return -1, nil, err
	}

	if mthd == "GET" && params != nil {
		q := reqURL.Query()
		for name := range params {
			if val, valOk := params[name].(string); valOk {
				q.Set(name, val)
			}
		}
		reqURL.RawQuery = q.Encode()
	}

	headers := map[string][]string{
		"Accept-Encoding": {"gzip, deflate"},
		"Accept-Language": {"en-us"},
		"Accept":          {"application/json"},
	}

	if c.Token != nil {
		headers["Authorization"] = []string{fmt.Sprintf("bearer %s", *c.Token)}
	}

	var req *http.Request

	if mthd == "POST" || mthd == "PUT" || mthd == "PATCH" {
		var payload []byte
		switch contentType {
		case "application/json":
			payload, err = json.Marshal(params)
			if err != nil {
				log.Warningf("Failed to marshal JSON payload for HTTP %s request; URL: %s; %s", method, urlString, err.Error())
				return -1, nil, err
			}

		case "application/x-www-form-urlencoded":
			urlEncodedForm := url.Values{}
			for key, val := range params {
				if valStr, valOk := val.(string); valOk {
					urlEncodedForm.Add(key, valStr)
				} else {
					log.Warningf("Failed to marshal application/x-www-form-urlencoded parameter: %s; value was non-string", key)
				}
			}
			payload = []byte(urlEncodedForm.Encode())

		case "multipart/form-data":
			body := new(bytes.Buffer)
			writer := multipart.NewWriter(body)
			for key, val := range params {
				if valStr, valStrOk := val.(string); valStrOk {
					dURL, err := dataurl.DecodeString(valStr)
					if err == nil {
						log.Debugf("Parsed data url parameter: %s", key)
						part, err := writer.CreateFormFile(key, key)
						if err != nil {
							return 0, nil, err
						}
						part.Write(dURL.Data)
					} else {
						_ = writer.WriteField(key, valStr)
					}
				} else {
					log.Warningf("Skipping non-string value when constructing multipart/form-data request: %s", key)
				}
			}
			err = writer.Close()
			if err != nil {
				return 0, nil, err
			}
			payload = []byte(body.Bytes())

		default:
			log.Warningf("Attempted HTTP %s request with unsupported content type: %s; unable to marshal request body", mthd, contentType)
		}

		req, _ = http.NewRequest(method, urlString, bytes.NewReader(payload))
		headers["Content-Type"] = []string{contentType}
	} else {
		req = &http.Request{
			URL:    reqURL,
			Method: mthd,
		}
	}

	req.Header = headers

	resp, err := client.Do(req)
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		log.Warningf("Failed to invoke HTTP %s request; URL: %s; %s", method, urlString, err.Error())
		return 0, nil, err
	}

	var reader io.ReadCloser
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
	default:
		reader = resp.Body
	}

	buf := new(bytes.Buffer)
	if reader != nil {
		defer reader.Close()
		buf.ReadFrom(reader)
	}

	if buf.Len() > 0 {
		contentTypeParts := strings.Split(resp.Header.Get("Content-Type"), ";")
		switch strings.ToLower(contentTypeParts[0]) {
		case "application/json":
			err = json.Unmarshal(buf.Bytes(), &response)
			if err != nil {
				return resp.StatusCode, nil, fmt.Errorf("Failed to unmarshal HTTP %s response; URL: %s; response: %s; %s", method, urlString, buf.Bytes(), err.Error())
			}
		case "binary/octet-stream":
			// try to unmarshal binary/octet-stream content as gz...
			reader, _ := gzip.NewReader(buf)
			buf := new(bytes.Buffer)
			if reader != nil {
				defer reader.Close()
				buf.ReadFrom(reader)
			}
			response = buf.Bytes()
		default:
			// no-op
		}
	}

	log.Debugf("Received %v response for HTTP %s request (%v-byte response received); URL: %s", resp.StatusCode, method, buf.Len(), urlString)
	return resp.StatusCode, response, nil
}

// Get constructs and synchronously sends an API GET request
func (c *Auth0APIClient) Get(uri string, params map[string]interface{}) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	return c.sendRequest("GET", url, defaultContentType, params)
}

// GetWithTLSClientConfig constructs and synchronously sends an API GET request
func (c *Auth0APIClient) GetWithTLSClientConfig(uri string, params map[string]interface{}, tlsClientConfig *tls.Config) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	return c.sendRequestWithTLSClientConfig("GET", url, defaultContentType, params, tlsClientConfig)
}

// Patch constructs and synchronously sends an API PATCH request
func (c *Auth0APIClient) Patch(uri string, params map[string]interface{}) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	return c.sendRequest("PATCH", url, defaultContentType, params)
}

// Post constructs and synchronously sends an API POST request
func (c *Auth0APIClient) Post(uri string, params map[string]interface{}) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	return c.sendRequest("POST", url, defaultContentType, params)
}

// PostWithTLSClientConfig constructs and synchronously sends an API POST request
func (c *Auth0APIClient) PostWithTLSClientConfig(uri string, params map[string]interface{}, tlsClientConfig *tls.Config) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	return c.sendRequestWithTLSClientConfig("POST", url, defaultContentType, params, tlsClientConfig)
}

// PostWWWFormURLEncoded constructs and synchronously sends an API POST request using application/x-www-form-urlencoded as the content-type
func (c *Auth0APIClient) PostWWWFormURLEncoded(uri string, params map[string]interface{}) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	return c.sendRequest("POST", url, "application/x-www-form-urlencoded", params)
}

// PostWWWFormURLEncodedWithTLSClientConfig constructs and synchronously sends an API POST request using application/x-www-form-urlencoded as the content-type
func (c *Auth0APIClient) PostWWWFormURLEncodedWithTLSClientConfig(uri string, params map[string]interface{}, tlsClientConfig *tls.Config) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	return c.sendRequestWithTLSClientConfig("POST", url, "application/x-www-form-urlencoded", params, tlsClientConfig)
}

// PostMultipartFormData constructs and synchronously sends an API POST request using multipart/form-data as the content-type
func (c *Auth0APIClient) PostMultipartFormData(uri string, params map[string]interface{}) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	return c.sendRequest("POST", url, "multipart/form-data", params)
}

// PostMultipartFormDataWithTLSClientConfig constructs and synchronously sends an API POST request using multipart/form-data as the content-type
func (c *Auth0APIClient) PostMultipartFormDataWithTLSClientConfig(uri string, params map[string]interface{}, tlsClientConfig *tls.Config) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	return c.sendRequestWithTLSClientConfig("POST", url, "multipart/form-data", params, tlsClientConfig)
}

// Put constructs and synchronously sends an API PUT request
func (c *Auth0APIClient) Put(uri string, params map[string]interface{}) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	return c.sendRequest("PUT", url, defaultContentType, params)
}

// PutWithTLSClientConfig constructs and synchronously sends an API PUT request
func (c *Auth0APIClient) PutWithTLSClientConfig(uri string, params map[string]interface{}, tlsClientConfig *tls.Config) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	return c.sendRequestWithTLSClientConfig("PUT", url, defaultContentType, params, tlsClientConfig)
}

// Delete constructs and synchronously sends an API DELETE request
func (c *Auth0APIClient) Delete(uri string) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	return c.sendRequest("DELETE", url, defaultContentType, nil)
}

// DeleteWithTLSClientConfig constructs and synchronously sends an API DELETE request
func (c *Auth0APIClient) DeleteWithTLSClientConfig(uri string, tlsClientConfig *tls.Config) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	return c.sendRequestWithTLSClientConfig("DELETE", url, defaultContentType, nil, tlsClientConfig)
}

func (c *Auth0APIClient) buildURL(uri string) string {
	path := c.Path
	if len(path) == 1 && path == "/" {
		path = ""
	} else if len(path) > 1 && strings.Index(path, "/") != 0 {
		path = fmt.Sprintf("/%s", path)
	}
	return fmt.Sprintf("%s://%s%s/%s", c.Scheme, c.Host, path, uri)
}

func buildBasicAuthorizationHeader(username, password string) string {
	auth := fmt.Sprintf("%s:%s", username, password)
	return fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(auth)))
}
