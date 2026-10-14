package api

import "net/http"

// CORS lets a browser page served from allowedOrigin (e.g. Swagger UI on
// http://localhost:8081) call this API. It is opt-in: with an empty
// allowedOrigin it returns next unchanged.
//
// CORS is enforced only by browsers. curl, Postman, tests and the worker never
// send an Origin header, so their requests pass straight through untouched.
//
// Exactly one origin is allowed (never "*"), only the methods the API uses,
// and only the Content-Type request header.
func CORS(allowedOrigin string, next http.Handler) http.Handler {
	if allowedOrigin == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Origin") != allowedOrigin {
			next.ServeHTTP(w, r) // not a browser call from the allowed page: no CORS headers
			return
		}

		h := w.Header()
		h.Set("Access-Control-Allow-Origin", allowedOrigin)
		h.Add("Vary", "Origin") // responses differ by Origin; keep caches honest
		h.Set("Access-Control-Expose-Headers", "Location")

		// Preflight: before a PATCH/DELETE or a JSON POST, the browser asks
		// with OPTIONS whether the real request is allowed. Answer and stop.
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			h.Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE")
			h.Set("Access-Control-Allow-Headers", "Content-Type")
			h.Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}