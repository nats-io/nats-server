// Copyright 2013 Apcera Inc. All rights reserved.

package server

import (
	"net/url"
	"sync"
)

type route struct {
	mu  sync.Mutex
	url *url.URL
}
