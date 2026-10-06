package certidp

import (
	"encoding/base64"
	"net/url"
	"strings"
	"testing"
)

func TestEncodeOCSPRequest(t *testing.T) {
	data := []byte("test data for OCSP request")
	encoded := encodeOCSPRequest(data)

	if strings.ContainsAny(encoded, "+/=") {
		t.Errorf("URL contains unescaped characters: %s", encoded)
	}

	decodedURL, err := url.QueryUnescape(encoded)
	if err != nil {
		t.Errorf("failed to url-decode request: %v", err)
	}
	decodedData, err := base64.StdEncoding.DecodeString(decodedURL)
	if err != nil {
		t.Errorf("failed to base64-decode request: %v", err)
	}
	if string(decodedData) != string(data) {
		t.Errorf("Decoded data does not match original: got %s, want %s", decodedData, data)
	}
}
