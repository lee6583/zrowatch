package user_management

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"transithub/backend/internal/shared/authctx"
	"transithub/backend/internal/shared/httpjson"
)

type AdminAccountResolver interface {
	RequireCurrentID(context.Context, string) (string, error)
}

type Handler struct {
	service  *Service
	accounts AdminAccountResolver
}

func RegisterRoutes(mux *http.ServeMux, service *Service, accounts AdminAccountResolver) {
	h := &Handler{service: service, accounts: accounts}
	mux.HandleFunc("GET /api/user-management/users", h.listUsers)
	mux.HandleFunc("PUT /api/user-management/users/{id}/rule", h.saveRule)
	mux.HandleFunc("DELETE /api/user-management/users/{id}/rule", h.deleteRule)
}

func (h *Handler) listUsers(w http.ResponseWriter, r *http.Request) {
	userID, adminID, ok := h.workspace(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	result, err := h.service.ListUsers(r.Context(), userID, adminID, UserQuery{
		Page: intQuery(q.Get("page"), 1), PageSize: intQuery(first(q.Get("page_size"), q.Get("pageSize")), 20),
		Status: q.Get("status"), Role: q.Get("role"), Search: q.Get("search"), SortBy: q.Get("sort_by"), SortOrder: q.Get("sort_order"), Timezone: q.Get("timezone"),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	httpjson.Write(w, http.StatusOK, result)
}

func (h *Handler) saveRule(w http.ResponseWriter, r *http.Request) {
	userID, adminID, ok := h.workspace(w, r)
	if !ok {
		return
	}
	var input RuleInput
	if err := httpjson.Decode(r, &input); err != nil {
		httpjson.WriteError(w, http.StatusBadRequest, string(ErrInvalidRequest))
		return
	}
	rule, err := h.service.SaveRule(r.Context(), userID, adminID, r.PathValue("id"), input)
	if err != nil {
		writeError(w, err)
		return
	}
	httpjson.Write(w, http.StatusOK, map[string]any{"rule": rule})
}

func (h *Handler) deleteRule(w http.ResponseWriter, r *http.Request) {
	userID, adminID, ok := h.workspace(w, r)
	if !ok {
		return
	}
	if err := h.service.DeleteRule(r.Context(), userID, adminID, r.PathValue("id")); err != nil {
		writeError(w, err)
		return
	}
	httpjson.Write(w, http.StatusOK, map[string]bool{"success": true})
}

func (h *Handler) workspace(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	userID, ok := authctx.UserID(r.Context())
	if !ok {
		httpjson.WriteError(w, http.StatusUnauthorized, "auth.errors.unauthorized")
		return "", "", false
	}
	adminID, err := h.accounts.RequireCurrentID(r.Context(), userID)
	if err != nil {
		httpjson.WriteError(w, http.StatusConflict, string(ErrNoCurrentAccount))
		return "", "", false
	}
	return userID, adminID, true
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	switch {
	case errors.Is(err, ErrInvalidRequest):
		status = http.StatusBadRequest
	case errors.Is(err, ErrUpstreamAuth):
		// This is the downstream Sub2API session, not the ZroWatch login token.
		// Keep the local admin signed in so they can repair the workspace session.
		status = http.StatusConflict
	case errors.Is(err, ErrPersistence):
		status = http.StatusInternalServerError
	}
	httpjson.WriteError(w, status, err.Error())
}

func intQuery(value string, fallback int) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return fallback
	}
	return parsed
}
func first(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
