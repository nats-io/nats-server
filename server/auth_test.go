// Copyright 2012-2018 The NATS Authors
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
	"reflect"
	"testing"
)

func TestUserCloneNilPermissions(t *testing.T) {
	user := &User{
		Username: "foo",
		Password: "bar",
	}

	clone := user.clone()

	if !reflect.DeepEqual(user, clone) {
		t.Fatalf("Cloned Users are incorrect.\nexpected: %+v\ngot: %+v",
			user, clone)
	}

	clone.Password = "baz"
	if reflect.DeepEqual(user, clone) {
		t.Fatal("Expected Users to be different")
	}
}

func TestUserClone(t *testing.T) {
	user := &User{
		Username: "foo",
		Password: "bar",
		Permissions: &Permissions{
			Publish:   []string{"foo"},
			Subscribe: []string{"bar"},
		},
	}

	clone := user.clone()

	if !reflect.DeepEqual(user, clone) {
		t.Fatalf("Cloned Users are incorrect.\nexpected: %+v\ngot: %+v",
			user, clone)
	}

	clone.Permissions.Subscribe = []string{"baz"}
	if reflect.DeepEqual(user, clone) {
		t.Fatal("Expected Users to be different")
	}
}

func TestUserClonePermissionsNoLists(t *testing.T) {
	user := &User{
		Username:    "foo",
		Password:    "bar",
		Permissions: &Permissions{},
	}

	clone := user.clone()

	if clone.Permissions.Publish != nil {
		t.Fatalf("Expected Publish to be nil, got: %v", clone.Permissions.Publish)
	}
	if clone.Permissions.Subscribe != nil {
		t.Fatalf("Expected Subscribe to be nil, got: %v", clone.Permissions.Subscribe)
	}
}

func TestUserCloneNoPermissions(t *testing.T) {
	user := &User{
		Username: "foo",
		Password: "bar",
	}

	clone := user.clone()

	if clone.Permissions != nil {
		t.Fatalf("Expected Permissions to be nil, got: %v", clone.Permissions)
	}
}

func TestUserCloneNil(t *testing.T) {
	user := (*User)(nil)
	clone := user.clone()
	if clone != nil {
		t.Fatalf("Expected nil, got: %+v", clone)
	}
}
