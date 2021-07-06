package server

import (
	"fmt"
	"strings"
)

type ErrorIdentifier uint16

// IsNatsErr determines if a error matches ID, if multiple IDs are given if the error matches any of these the function will be true
func IsNatsErr(err error, ids ...ErrorIdentifier) bool {
	if err == nil {
		return false
	}

	ce, ok := err.(*ApiError)
	if !ok || ce == nil {
		return false
	}

	for _, id := range ids {
		ae, ok := ApiErrors[id]
		if !ok || ae == nil {
			continue
		}

		if ce.ErrCode == ae.ErrCode {
			return true
		}
	}

	return false
}

// ApiError is included in all responses if there was an error.
type ApiError struct {
	Code        int    `json:"code"`
	ErrCode     uint16 `json:"err_code,omitempty"`
	Description string `json:"description,omitempty"`
}

// ErrorsData is the source data for generated errors as found in errors.json
type ErrorsData struct {
	Constant    string `json:"constant"`
	Code        int    `json:"code"`
	ErrCode     uint16 `json:"error_code"`
	Description string `json:"description"`
	Comment     string `json:"comment"`
	Help        string `json:"help"`
	URL         string `json:"url"`
	Deprecates  string `json:"deprecates"`
}

func (e *ApiError) Error() string {
	return fmt.Sprintf("%s (%d)", e.Description, e.ErrCode)
}

// ErrOrNewT returns err if it's an ApiError else creates a new error using NewT()
func (e *ApiError) ErrOrNewT(err error, replacements ...interface{}) *ApiError {
	if ae, ok := err.(*ApiError); ok {
		return ae
	}

	return e.NewT(replacements...)
}

// ErrOr returns err if it's an ApiError else creates a new error
func (e *ApiError) ErrOr(err error) *ApiError {
	if ae, ok := err.(*ApiError); ok {
		return ae
	}

	return e
}

// NewT creates a new error using strings.Replacer on the Description field, arguments must be an even number like NewT("{err}", err)
func (e *ApiError) NewT(replacements ...interface{}) *ApiError {
	ne := &ApiError{
		Code:        e.Code,
		ErrCode:     e.ErrCode,
		Description: e.Description,
	}

	if len(replacements) == 0 {
		return ne
	}

	if len(replacements)%2 != 0 {
		panic("invalid error replacement")
	}

	var ra []string

	var key string
	for i, replacement := range replacements {
		if i%2 == 0 {
			key = replacement.(string)
			continue
		}

		switch v := replacement.(type) {
		case string:
			ra = append(ra, key, v)
		case error:
			ra = append(ra, key, v.Error())
		default:
			ra = append(ra, key, fmt.Sprintf("%v", v))
		}
	}

	ne.Description = strings.NewReplacer(ra...).Replace(e.Description)

	return ne
}
