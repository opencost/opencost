package configrbac

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/protocol"
	"github.com/opencost/opencost/core/pkg/util/json"
)

var proto = protocol.HTTP()

// Handler serves /config/rbac/* endpoints.
type Handler struct {
	svc *Service
}

// NewHandler creates HTTP handlers for RBAC APIs.
func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) writeDisabled(w http.ResponseWriter) {
	proto.WriteError(w, proto.NotImplemented("scoped views API is disabled in config.json"))
}

func (h *Handler) writeDisabledIf(err error, w http.ResponseWriter) bool {
	if errors.Is(err, ErrScopedViewsDisabled) {
		h.writeDisabled(w)
		return true
	}
	return false
}

func (h *Handler) writeServiceError(w http.ResponseWriter, err error) {
	if IsNotFound(err) {
		proto.WriteError(w, proto.NotFound())
		return
	}
	if errors.Is(err, ErrDuplicateID) {
		proto.WriteError(w, protocol.HTTPError{StatusCode: http.StatusConflict, Body: err.Error()})
		return
	}
	proto.WriteError(w, proto.BadRequest(err.Error()))
}

func setCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

// PostScopedView handles POST /config/rbac/scopedViews.
func (h *Handler) PostScopedView(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	setCORS(w)

	var view ScopedView
	if err := decodeJSONBody(r, &view); err != nil {
		proto.WriteError(w, proto.BadRequest(err.Error()))
		return
	}

	created, err := h.svc.Create(view)
	if h.writeDisabledIf(err, w) {
		return
	}
	if err != nil {
		h.writeServiceError(w, err)
		return
	}

	proto.WriteData(w, created)
}

// PutScopedView handles PUT /config/rbac/scopedViews/:id.
func (h *Handler) PutScopedView(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	setCORS(w)

	id := strings.TrimSpace(ps.ByName("id"))
	var view ScopedView
	if err := decodeJSONBody(r, &view); err != nil {
		proto.WriteError(w, proto.BadRequest(err.Error()))
		return
	}

	updated, err := h.svc.Update(id, view)
	if h.writeDisabledIf(err, w) {
		return
	}
	if err != nil {
		h.writeServiceError(w, err)
		return
	}

	proto.WriteData(w, updated)
}

// GetScopedViews handles GET /config/rbac/scopedViews and GET /config/rbac/scopedViews/:id.
func (h *Handler) GetScopedViews(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	setCORS(w)

	id := strings.TrimSpace(ps.ByName("id"))
	if id == "" {
		id = strings.TrimSpace(r.URL.Query().Get("id"))
	}

	if id != "" {
		view, err := h.svc.Get(id)
		if h.writeDisabledIf(err, w) {
			return
		}
		if err != nil {
			h.writeServiceError(w, err)
			return
		}
		proto.WriteData(w, view)
		return
	}

	views, err := h.svc.List()
	if h.writeDisabledIf(err, w) {
		return
	}
	if err != nil {
		proto.WriteError(w, proto.InternalServerError(err.Error()))
		return
	}
	proto.WriteData(w, views)
}

// DeleteScopedView handles DELETE /config/rbac/scopedViews/:id.
func (h *Handler) DeleteScopedView(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	setCORS(w)

	id := strings.TrimSpace(ps.ByName("id"))
	err := h.svc.Delete(id)
	if h.writeDisabledIf(err, w) {
		return
	}
	if err != nil {
		h.writeServiceError(w, err)
		return
	}

	proto.WriteRawNoContent(w)
}

// GetUserPolicy handles GET /config/rbac/policy/users/:userId.
func (h *Handler) GetUserPolicy(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	setCORS(w)

	userID := strings.TrimSpace(ps.ByName("userId"))
	policy, err := h.svc.ResolvePolicy(userID)
	if h.writeDisabledIf(err, w) {
		return
	}
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	proto.WriteData(w, policy)
}

func decodeJSONBody(r *http.Request, dest interface{}) error {
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return errors.New("request body is required")
	}
	if err := json.Unmarshal(body, dest); err != nil {
		return errors.New("invalid JSON body")
	}
	return nil
}
