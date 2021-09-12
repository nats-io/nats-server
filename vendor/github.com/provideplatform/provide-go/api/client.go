package api

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/provideplatform/provide-go/common"
	"github.com/vincent-petithory/dataurl"
)

const defaultContentType = "application/json"
const defaultRequestTimeout = time.Second * 10

var customRequestTimeout *time.Duration

// Client is a generic base class for calling a REST API; when a token is configured on an
// Client instance it will be provided as a bearer authorization header; when a username and
// password are configured on an Client instance, they will be used for HTTP basic authorization
// but will be passed as the Authorization header instead of as part of the URL itself. When a token
// is configured on an Client instance, the username and password supplied for basic auth are
// currently discarded.
type Client struct {
	Host   string
	Path   string
	Scheme string

	Cookie  *string
	Headers map[string][]string
	Token   *string

	Username *string
	Password *string
}

func requestTimeout() time.Duration {
	if customRequestTimeout != nil {
		return *customRequestTimeout
	}

	envRequestTimeout := os.Getenv("REQUEST_TIMEOUT")
	if envRequestTimeout == "" {
		return defaultRequestTimeout
	}

	timeout, err := strconv.ParseInt(envRequestTimeout, 10, 64)
	if err != nil {
		common.Log.Debugf("error parsing custom request timeout; using default timeout of %v seconds; %s", defaultRequestTimeout, err.Error())
		return defaultRequestTimeout
	}

	timeoutInSeconds := time.Duration(timeout) * time.Second

	common.Log.Debugf("using custom timeout of %v seconds for requests", timeout)
	customRequestTimeout = &timeoutInSeconds

	return *customRequestTimeout
}

func (c *Client) parseResponse(resp *http.Response) (status int, response interface{}, err error) {
	if resp == nil {
		return 0, nil, errors.New("nil response")
	}

	if resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		common.Log.Warningf("failed to invoke HTTP %s request: %s; %s", resp.Request.Method, resp.Request.URL.String(), err.Error())
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

		for {
			buffer := make([]byte, 256)
			n, err := reader.Read(buffer)
			if n > 0 {
				common.Log.Tracef("read %d bytes from HTTP response stream", n)
				i, err := buf.Write(buffer[0:n])
				if err != nil {
					common.Log.Warningf("failed to write HTTP response to internal client buffer; %s", err.Error())
					return resp.StatusCode, nil, err
				} else {
					common.Log.Tracef("wrote %d bytes from HTTP response to internal client buffer", i)
				}
			} else if err != nil {
				if err != io.EOF {
					common.Log.Warningf("failed to read HTTP response stream; %s", err.Error())
					return resp.StatusCode, nil, err
				}
				break
			}
		}
	}

	if buf.Len() > 0 {
		contentTypeParts := strings.Split(resp.Header.Get("Content-Type"), ";")
		switch strings.ToLower(contentTypeParts[0]) {
		case "application/json":
			err = json.Unmarshal(buf.Bytes(), &response)
			if err != nil {
				err = fmt.Errorf("failed to unmarshal %v-byte HTTP %s response from %s; %s", len(buf.Bytes()), resp.Request.Method, resp.Request.URL.String(), err.Error())
				return resp.StatusCode, nil, err
			}
		default:
			// no-op
		}
	}

	return resp.StatusCode, response, nil
}

func (c *Client) sendRequest(
	method,
	urlString,
	contentType string,
	params map[string]interface{},
) (resp *http.Response, err error) {
	return c.sendRequestWithTLSClientConfig(method, urlString, contentType, params,
		&tls.Config{
			InsecureSkipVerify: false,
		},
	)
}

func (c *Client) sendRequestWithTLSClientConfig(
	method,
	urlString,
	contentType string,
	params map[string]interface{},
	tlsClientConfig *tls.Config,
) (resp *http.Response, err error) {
	client := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
			TLSClientConfig:   tlsClientConfig,
		},
		Timeout: requestTimeout(),
	}

	mthd := strings.ToUpper(method)
	reqURL, err := url.Parse(urlString)
	if err != nil {
		common.Log.Warningf("failed to parse URL for HTTP %s request: %s; %s", method, urlString, err.Error())
		return nil, err
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
	} else if c.Username != nil && c.Password != nil {
		headers["Authorization"] = []string{buildBasicAuthorizationHeader(*c.Username, *c.Password)}
	}

	if c.Cookie != nil {
		headers["Cookie"] = []string{*c.Cookie}
	}

	if c.Headers != nil {
		for name, val := range c.Headers {
			headers[name] = val
		}
	}

	var req *http.Request

	if mthd == "POST" || mthd == "PUT" || mthd == "PATCH" {
		var payload []byte
		switch contentType {
		case "application/json":
			payload, err = json.Marshal(params)
			if err != nil {
				common.Log.Warningf("failed to marshal JSON payload for HTTP %s request: %s; %s", method, urlString, err.Error())
				return nil, err
			}

		case "application/x-www-form-urlencoded":
			urlEncodedForm := url.Values{}
			for key, val := range params {
				if valStr, valOk := val.(string); valOk {
					urlEncodedForm.Add(key, valStr)
				} else {
					common.Log.Warningf("failed to marshal application/x-www-form-urlencoded parameter: %s; value was non-string", key)
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
						common.Log.Tracef("parsed data url parameter: %s", key)
						part, err := writer.CreateFormFile(key, key)
						if err != nil {
							return nil, err
						}
						part.Write(dURL.Data)
					} else {
						_ = writer.WriteField(key, valStr)
					}
				} else {
					common.Log.Warningf("skipping non-string value when constructing multipart/form-data request: %s", key)
				}
			}
			err = writer.Close()
			if err != nil {
				return nil, err
			}
			payload = []byte(body.Bytes())

		default:
			common.Log.Warningf("attempted HTTP %s request with unsupported content type: %s; unable to marshal request body", mthd, contentType)
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
	return client.Do(req)
}

// Get constructs and synchronously sends an API GET request
func (c *Client) Get(uri string, params map[string]interface{}) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	resp, err := c.sendRequest("GET", url, defaultContentType, params)
	return c.parseResponse(resp)
}

// Head constructs and synchronously sends an API HEAD request; returns the headers
func (c *Client) Head(uri string, params map[string]interface{}) (status int, response map[string][]string, err error) {
	url := c.buildURL(uri)
	resp, err := c.sendRequest("HEAD", url, defaultContentType, params)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, resp.Header, nil
}

// GetWithTLSClientConfig constructs and synchronously sends an API GET request
func (c *Client) GetWithTLSClientConfig(uri string, params map[string]interface{}, tlsClientConfig *tls.Config) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	resp, err := c.sendRequestWithTLSClientConfig("GET", url, defaultContentType, params, tlsClientConfig)
	return c.parseResponse(resp)
}

// Patch constructs and synchronously sends an API PATCH request
func (c *Client) Patch(uri string, params map[string]interface{}) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	resp, err := c.sendRequest("PATCH", url, defaultContentType, params)
	return c.parseResponse(resp)
}

// PatchWithTLSClientConfig constructs and synchronously sends an API PATCH request
func (c *Client) PatchWithTLSClientConfig(uri string, params map[string]interface{}, tlsClientConfig *tls.Config) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	resp, err := c.sendRequestWithTLSClientConfig("PATCH", url, defaultContentType, params, tlsClientConfig)
	return c.parseResponse(resp)
}

// Post constructs and synchronously sends an API POST request
func (c *Client) Post(uri string, params map[string]interface{}) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	resp, err := c.sendRequest("POST", url, defaultContentType, params)
	return c.parseResponse(resp)
}

// PostWithTLSClientConfig constructs and synchronously sends an API POST request
func (c *Client) PostWithTLSClientConfig(uri string, params map[string]interface{}, tlsClientConfig *tls.Config) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	resp, err := c.sendRequestWithTLSClientConfig("POST", url, defaultContentType, params, tlsClientConfig)
	return c.parseResponse(resp)
}

// PostWWWFormURLEncoded constructs and synchronously sends an API POST request using application/x-www-form-urlencoded as the content-type
func (c *Client) PostWWWFormURLEncoded(uri string, params map[string]interface{}) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	resp, err := c.sendRequest("POST", url, "application/x-www-form-urlencoded", params)
	return c.parseResponse(resp)
}

// PostWWWFormURLEncodedWithTLSClientConfig constructs and synchronously sends an API POST request using application/x-www-form-urlencoded as the content-type
func (c *Client) PostWWWFormURLEncodedWithTLSClientConfig(uri string, params map[string]interface{}, tlsClientConfig *tls.Config) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	resp, err := c.sendRequestWithTLSClientConfig("POST", url, "application/x-www-form-urlencoded", params, tlsClientConfig)
	return c.parseResponse(resp)
}

// PostMultipartFormData constructs and synchronously sends an API POST request using multipart/form-data as the content-type
func (c *Client) PostMultipartFormData(uri string, params map[string]interface{}) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	resp, err := c.sendRequest("POST", url, "multipart/form-data", params)
	return c.parseResponse(resp)
}

// PostMultipartFormDataWithTLSClientConfig constructs and synchronously sends an API POST request using multipart/form-data as the content-type
func (c *Client) PostMultipartFormDataWithTLSClientConfig(uri string, params map[string]interface{}, tlsClientConfig *tls.Config) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	resp, err := c.sendRequestWithTLSClientConfig("POST", url, "multipart/form-data", params, tlsClientConfig)
	return c.parseResponse(resp)
}

// Put constructs and synchronously sends an API PUT request
func (c *Client) Put(uri string, params map[string]interface{}) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	resp, err := c.sendRequest("PUT", url, defaultContentType, params)
	return c.parseResponse(resp)
}

// PutWithTLSClientConfig constructs and synchronously sends an API PUT request
func (c *Client) PutWithTLSClientConfig(uri string, params map[string]interface{}, tlsClientConfig *tls.Config) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	resp, err := c.sendRequestWithTLSClientConfig("PUT", url, defaultContentType, params, tlsClientConfig)
	return c.parseResponse(resp)
}

// Delete constructs and synchronously sends an API DELETE request
func (c *Client) Delete(uri string) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	resp, err := c.sendRequest("DELETE", url, defaultContentType, nil)
	return c.parseResponse(resp)
}

// DeleteWithTLSClientConfig constructs and synchronously sends an API DELETE request
func (c *Client) DeleteWithTLSClientConfig(uri string, tlsClientConfig *tls.Config) (status int, response interface{}, err error) {
	url := c.buildURL(uri)
	resp, err := c.sendRequestWithTLSClientConfig("DELETE", url, defaultContentType, nil, tlsClientConfig)
	return c.parseResponse(resp)
}

func (c *Client) buildURL(uri string) string {
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
