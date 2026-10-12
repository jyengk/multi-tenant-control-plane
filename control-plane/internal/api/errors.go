package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"controlplane/internal/domain"
)

// Transport-level error codes. Domain codes come from the domain package;
// these cover failures that never reach the domain.
const (
	codeInvalidRequest   = "invalid_request"    // malformed JSON, wrong content type
	codeRouteNotFound    = "route_not_found"    // no such path
	codeMethodNotAllowed = "method_not_allowed" // path exists, method doesn't
	codeInternal         = "internal_error"     // anything unexpected
)

// httpStatusFor is the single, documented mapping from domain error codes
// to HTTP statuses. Keep it in sync with the README.
var httpStatusFor = map[domain.Code]int{
	domain.CodeTenantNotFound:         http.StatusNotFound,
	domain.CodeTaskNotFound:           http.StatusNotFound,
	domain.CodeTenantAlreadyExists:    http.StatusConflict,
	domain.CodeTenantUpdateNotAllowed: http.StatusConflict,
	domain.CodeTenantVersionConflict:  http.StatusConflict,
	domain.CodeValidation:             http.StatusUnprocessableEntity,
}

// errorBody is the one error shape every endpoint returns:
//
//	{"error": {"code": "tenant_not_found", "message": "...", "details": {...}}}
type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Details map[string]string `json:"details,omitempty"`
}

// writeError maps err to a structured response. Domain errors keep their
// code and message; anything else is logged and reported as a generic 500
// so internals (SQL, stack traces) never reach the client.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	var de *domain.Error
	if errors.As(err, &de) {
		status, ok := httpStatusFor[de.Code]
		if !ok {
			status = http.StatusInternalServerError
		}
		writeErrorBody(w, status, string(de.Code), de.Message, de.Fields)
		return
	}
	slog.ErrorContext(r.Context(), "unhandled error", "err", err, "method", r.Method, "path", r.URL.Path)
	writeErrorBody(w, http.StatusInternalServerError, codeInternal, "internal server error", nil)
}

func writeErrorBody(w http.ResponseWriter, status int, code, msg string, details map[string]string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: msg, Details: details}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encode response", "err", err)
	}
}
