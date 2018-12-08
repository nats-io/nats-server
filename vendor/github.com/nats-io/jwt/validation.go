package jwt

import (
	"errors"
	"fmt"
)

// ValidationIssue represents an issue during JWT validation, it may or may not be a blocking error
type ValidationIssue struct {
	Description string
	Blocking    bool
	TimeCheck   bool
}

func (ve *ValidationIssue) Error() string {
	return ve.Description
}

// ValidationResults is a list of ValidationIssue pointers
type ValidationResults struct {
	Issues []*ValidationIssue
}

// CreateValidationResults creates an empty list of validation issues
func CreateValidationResults() *ValidationResults {
	issues := []*ValidationIssue{}
	return &ValidationResults{
		Issues: issues,
	}
}

//Add appends an issue to the list
func (v *ValidationResults) Add(vi *ValidationIssue) {
	v.Issues = append(v.Issues, vi)
}

// AddError creates a new validation error and adds it to the list
func (v *ValidationResults) AddError(format string, args ...interface{}) {
	v.Add(&ValidationIssue{
		Description: fmt.Sprintf(format, args...),
		Blocking:    true,
		TimeCheck:   false,
	})
}

// AddTimeCheck creates a new validation issue related to a time check and adds it to the list
func (v *ValidationResults) AddTimeCheck(format string, args ...interface{}) {
	v.Add(&ValidationIssue{
		Description: fmt.Sprintf(format, args...),
		Blocking:    false,
		TimeCheck:   true,
	})
}

// AddWarning creates a new validation warning and adds it to the list
func (v *ValidationResults) AddWarning(format string, args ...interface{}) {
	v.Add(&ValidationIssue{
		Description: fmt.Sprintf(format, args...),
		Blocking:    false,
		TimeCheck:   false,
	})
}

// IsBlocking returns true if the list contains a blocking error
func (v *ValidationResults) IsBlocking(includeTimeChecks bool) bool {
	for _, i := range v.Issues {
		if i.Blocking {
			return true
		}

		if includeTimeChecks && i.TimeCheck {
			return true
		}
	}
	return false
}

// IsEmpty returns true if the list is empty
func (v *ValidationResults) IsEmpty() bool {
	return len(v.Issues) == 0
}

// Errors returns only blocking issues as errors
func (v *ValidationResults) Errors() []error {
	var errs []error
	for _, v := range v.Issues {
		if v.Blocking {
			errs = append(errs, errors.New(v.Description))
		}
	}
	return errs
}
