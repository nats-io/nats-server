// Copyright 2013 Apcera Inc. All rights reserved.

// Conf defines a generalized map for configuration

package conf

import (
//	"errors"
)

type Conf map[string]interface{}
type arr []interface{}

/*
func Parse(confPath string) (Conf, error) {
	return nil, errors.New("Need impl")
}

func ParseString(conf string) (Conf, error) {
	c := Conf(make(map[string]interface{}))

	return parse(c, conf)
}
*/
