// Copyright 2018-2024 The NATS Authors
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

package server

import (
	"fmt"
	"time"
)

// CustomLookupCacheDirAccResolver is based on the FULL CacheDirAccResolver and will lookup the account every time by sending a message to $SYS.REQ.ACCOUNT.*.CLAIMS.LOOKUP
type CustomLookupCacheDirAccResolver struct {
	*CacheDirAccResolver
}

var _ AccountResolver = (*CustomLookupCacheDirAccResolver)(nil)

func NewCustomLookupCacheDirAccResolver(path string, limit int64, ttl time.Duration, opts ...DirResOption) (*CustomLookupCacheDirAccResolver, error) {
	dirAccResolver, err := NewCacheDirAccResolver(path, limit, ttl, opts...)
	if err != nil {
		return nil, err
	}
	return &CustomLookupCacheDirAccResolver{
		CacheDirAccResolver: dirAccResolver,
	}, nil
}
func (ca *CustomLookupCacheDirAccResolver) Reload() error {
	return ca.DirAccResolver.Reload()
}
func (ca *CustomLookupCacheDirAccResolver) Start(s *Server) error {
	ca.Server = s
	return nil
}
func (ca *CustomLookupCacheDirAccResolver) Fetch(name string) (string, error) {

	res, err := ca.DirAccResolver.Fetch(name)
	if err == nil {
		return res, nil
	}
	// we have an error file not found

	if err == ErrMissingAccount {
		ss, err := ca.Server.fetch(ca, name, ca.fetchTimeout)
		return ss, err
	}
	return _EMPTY_, ErrMissingAccount
}

// CustomLookupDirAccResolver is based on the FULL DirAccResolver and will lookup the account every time by sending a message to $SYS.REQ.ACCOUNT.*.CLAIMS.LOOKUP
// this is primarily to not go through the operatorMode flow paths.
type CustomLookupDirAccResolver struct {
	*DirAccResolver
}

var _ AccountResolver = (*CustomLookupDirAccResolver)(nil)

func NewCustomLookupDirAccResolver(path string, limit int64, syncInterval time.Duration, delete deleteType, opts ...DirResOption) (*CustomLookupDirAccResolver, error) {
	dirAccResolver, err := NewDirAccResolver(path, limit, syncInterval, delete, opts...)
	if err != nil {
		return nil, err
	}
	return &CustomLookupDirAccResolver{
		DirAccResolver: dirAccResolver,
	}, nil
}
func (ca *CustomLookupDirAccResolver) Reload() error {
	return ca.DirAccResolver.Reload()
}
func (ca *CustomLookupDirAccResolver) Start(s *Server) error {
	ca.Server = s
	return nil
}
func (ca *CustomLookupDirAccResolver) Fetch(name string) (string, error) {

	res, err := ca.DirAccResolver.Fetch(name)
	if err == nil {
		return res, nil
	}
	// we have an error file not found

	if err == ErrMissingAccount {
		ss, err := ca.Server.fetch(ca, name, ca.fetchTimeout)
		return ss, err
	}
	return _EMPTY_, ErrMissingAccount
}

// CustomLookupAccResolver is an account resolver that will lookup the account every time by sending a message to $SYS.REQ.ACCOUNT.*.CLAIMS.LOOKUP
type CustomLookupAccResolver struct {
	*Server
	fetchTimeout time.Duration
	resolverDefaultsOpsImpl
}

var _ AccountResolver = (*CustomLookupAccResolver)(nil)

type CustomLookupResOption func(s *CustomLookupAccResolver) error

// limits the amount of time spent waiting for an account fetch to complete
func CustomLookupResFetchTimeout(to time.Duration) CustomLookupResOption {
	return func(r *CustomLookupAccResolver) error {
		if to <= time.Duration(0) {
			return fmt.Errorf("Fetch timeout %v is too smal", to)
		}
		r.fetchTimeout = to
		return nil
	}
}
func NewCustomLookupAccResolver(opt ...CustomLookupResOption) (*CustomLookupAccResolver, error) {
	res := &CustomLookupAccResolver{
		fetchTimeout: DEFAULT_ACCOUNT_FETCH_TIMEOUT,
	}
	for _, o := range opt {
		if err := o(res); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (ca *CustomLookupAccResolver) Start(s *Server) error {
	ca.Server = s
	return nil
}
func (*CustomLookupAccResolver) Reload() error {
	return nil
}
func (*CustomLookupAccResolver) Store(_, _ string) error {
	return nil
}
func (ca *CustomLookupAccResolver) Fetch(name string) (string, error) {
	ss, err := ca.Server.fetch(ca, name, ca.fetchTimeout)
	return ss, err
}
