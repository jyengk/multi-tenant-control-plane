package domain

import (
	"regexp"
	"strings"
)

// slugPattern is k8s-namespace compatible (DNS label), 3-28 chars. The same
// rule is enforced by a CHECK constraint in the database.
var slugPattern = regexp.MustCompile(`^[a-z][-a-z0-9]{1,26}[a-z0-9]$`)

// ValidateSlug reports whether slug is well-formed. Uniqueness is the
// database's job (UNIQUE constraint), not this function's.
func ValidateSlug(slug string) bool { return slugPattern.MatchString(slug) }

// ValidateName reports whether name is non-empty after trimming whitespace.
func ValidateName(name string) bool { return strings.TrimSpace(name) != "" }
