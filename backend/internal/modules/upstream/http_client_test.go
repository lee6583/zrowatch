package upstream

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestJSONPreservesStatusForNonJSONErrorResponse(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		messageKey string
	}{
		{name: "unauthorized", statusCode: http.StatusUnauthorized, messageKey: ErrorAuth},
		{name: "not found", statusCode: http.StatusNotFound, messageKey: ErrorRequest},
		{name: "bad gateway", statusCode: http.StatusBadGateway, messageKey: ErrorRequest},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(test.statusCode)
				_, _ = w.Write([]byte("<html>upstream error</html>"))
			}))
			defer server.Close()

			_, err := NewHTTPClient(server.Client()).requestJSON(server.URL, requestOptions{})
			var requestErr *RequestError
			if !errors.As(err, &requestErr) {
				t.Fatalf("expected RequestError, got %T: %v", err, err)
			}
			if requestErr.StatusCode != test.statusCode || requestErr.MessageKey != test.messageKey {
				t.Fatalf("unexpected error: %#v", requestErr)
			}
		})
	}
}

func TestRequestJSONContextHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewHTTPClient(http.DefaultClient).requestJSONContext(ctx, "https://example.com", requestOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}
