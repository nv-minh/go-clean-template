package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/yourorg/go-clean-template/internal/domain"
	"github.com/yourorg/go-clean-template/internal/platform/logger"
)

// problem is an RFC 9457 (ex RFC 7807) "problem details" body.
type problem struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Detail    string `json:"detail,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

func writeProblem(w http.ResponseWriter, r *http.Request, status int, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem{
		Type:      "about:blank",
		Title:     title,
		Status:    status,
		Detail:    detail,
		RequestID: logger.RequestID(r.Context()),
	})
}

// writeError maps domain errors to HTTP statuses. Unknown errors are logged with full detail
// but answered with a generic 500 so internals never leak to clients.
func writeError(w http.ResponseWriter, r *http.Request, log *slog.Logger, err error) {
	switch {
	case errors.Is(err, domain.ErrInvalid):
		writeProblem(w, r, http.StatusUnprocessableEntity, "Validation failed", err.Error())
	case errors.Is(err, domain.ErrNotFound):
		writeProblem(w, r, http.StatusNotFound, "Not found", "")
	case errors.Is(err, domain.ErrConflict), errors.Is(err, domain.ErrInvalidState):
		writeProblem(w, r, http.StatusConflict, "Conflict", err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		writeProblem(w, r, http.StatusGatewayTimeout, "Request timed out", "")
	case errors.Is(err, context.Canceled):
		// Client went away; nothing useful to send. 499 is the nginx convention.
		w.WriteHeader(499)
	default:
		log.ErrorContext(r.Context(), "unhandled error", slog.Any("error", err))
		writeProblem(w, r, http.StatusInternalServerError, "Internal server error", "")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// decodeJSON strictly decodes a single JSON document: unknown fields and trailing data are rejected.
func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain a single JSON document")
	}
	return nil
}

func writeDecodeError(w http.ResponseWriter, r *http.Request, err error) {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		writeProblem(w, r, http.StatusRequestEntityTooLarge, "Payload too large", "")
		return
	}
	writeProblem(w, r, http.StatusBadRequest, "Malformed request body", err.Error())
}
