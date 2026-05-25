package configrbac

import "github.com/julienschmidt/httprouter"

// RegisterRoutes mounts scoped view endpoints on the router.
// Mutations are wrapped with writeAuth when non-nil (e.g. admin token middleware).
func RegisterRoutes(router *httprouter.Router, writeAuth func(httprouter.Handle) httprouter.Handle) {
	h := NewHandler(NewService(NewConfigLoader(), NewStore()))

	wrapWrite := func(handle httprouter.Handle) httprouter.Handle {
		if writeAuth != nil {
			return writeAuth(handle)
		}
		return handle
	}

	router.POST("/config/rbac/scopedViews", wrapWrite(h.PostScopedView))
	router.PUT("/config/rbac/scopedViews/:id", wrapWrite(h.PutScopedView))
	router.DELETE("/config/rbac/scopedViews/:id", wrapWrite(h.DeleteScopedView))
	router.GET("/config/rbac/scopedViews", h.GetScopedViews)
	router.GET("/config/rbac/scopedViews/:id", h.GetScopedViews)

	router.GET("/config/rbac/policy/users/:userId", h.GetUserPolicy)
}
