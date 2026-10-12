package domain

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateSlug(t *testing.T) {
	valid := []string{
		"abc", "acme-eu", "a1b", "tenant-42",
		"a" + strings.Repeat("b", 27), // 28 chars, the maximum
	}
	invalid := []string{
		"", "ab", // too short
		"a" + strings.Repeat("b", 28), // 29 chars
		"1abc",                        // must start with a letter
		"-abc",                        // must start with a letter
		"abc-",                        // must not end with a hyphen
		"Acme",                        // uppercase
		"ac_me", "ac.me", "ac me",     // illegal characters
	}
	for _, s := range valid {
		if !ValidateSlug(s) {
			t.Errorf("%q should be valid", s)
		}
	}
	for _, s := range invalid {
		if ValidateSlug(s) {
			t.Errorf("%q should be invalid", s)
		}
	}
}

func TestValidateName(t *testing.T) {
	if !ValidateName("Acme Corp") {
		t.Error("normal name should be valid")
	}
	for _, s := range []string{"", "   ", "\t\n"} {
		if ValidateName(s) {
			t.Errorf("%q should be invalid", s)
		}
	}
}

func TestErrorIsMatchesByCode(t *testing.T) {
	v := NewValidationError(map[string]string{"slug": "invalid format"})
	if !errors.Is(v, ErrValidation) {
		t.Error("validation error with fields should match ErrValidation")
	}
	if errors.Is(v, ErrTenantNotFound) {
		t.Error("different codes must not match")
	}
}
