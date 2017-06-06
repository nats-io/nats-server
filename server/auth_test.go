// Copyright 2017 Apcera Inc. All rights reserved.

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

func TestUserCloneNil(t *testing.T) {
	user := (*User)(nil)
	clone := user.clone()
	if clone != nil {
		t.Fatalf("Expected nil, got: %+v", clone)
	}
}
