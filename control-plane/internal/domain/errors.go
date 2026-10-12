package domain

// Code is a machine-readable error code. The values are part of the API
// contract and must not change.
type Code string

// Error codes. The first five are fixed by the brief; validation_error is ours.
const (
	CodeTenantNotFound         Code = "tenant_not_found"
	CodeTenantAlreadyExists    Code = "tenant_already_exists"
	CodeTenantUpdateNotAllowed Code = "tenant_update_not_allowed"
	CodeTenantVersionConflict  Code = "tenant_version_conflict"
	CodeTaskNotFound           Code = "task_not_found"
	CodeValidation             Code = "validation_error"
)

// Error is a domain error carrying a contract code. The API layer maps codes
// to HTTP statuses; nothing below it knows about HTTP.
type Error struct {
	Code    Code
	Message string
	Fields  map[string]string // per-field details, used by validation errors
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

// Is lets errors.Is match on code, so a validation error with field details
// still matches ErrValidation.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

// Sentinel errors, compared with errors.Is.
var (
	ErrTenantNotFound         = &Error{Code: CodeTenantNotFound, Message: "tenant not found"}
	ErrTenantAlreadyExists    = &Error{Code: CodeTenantAlreadyExists, Message: "a tenant with this slug already exists"}
	ErrTenantUpdateNotAllowed = &Error{Code: CodeTenantUpdateNotAllowed, Message: "operation not allowed in the tenant's current status"}
	ErrTenantVersionConflict  = &Error{Code: CodeTenantVersionConflict, Message: "tenant version does not match; refetch and retry"}
	ErrTaskNotFound           = &Error{Code: CodeTaskNotFound, Message: "task not found"}
	ErrValidation             = &Error{Code: CodeValidation, Message: "request validation failed"}
)

// NewValidationError builds a validation error with per-field details.
func NewValidationError(fields map[string]string) *Error {
	return &Error{Code: CodeValidation, Message: ErrValidation.Message, Fields: fields}
}
