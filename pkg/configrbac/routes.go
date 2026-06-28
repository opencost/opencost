package configrbac

import "github.com/julienschmidt/httprouter"

// RegisterRoutes mounts scoped view endpoints on the router.
// Management endpoints are wrapped with adminAuth when non-nil (e.g. admin token middleware).
func RegisterRoutes(router *httprouter.Router, adminAuth func(httprouter.Handle) httprouter.Handle) {
	h := NewHandler(NewService(NewConfigLoader(), NewStore()))

	wrapAdmin := func(handle httprouter.Handle) httprouter.Handle {
		if adminAuth != nil {
			return adminAuth(handle)
		}
		return handle
	}

	router.POST("/config/rbac/scopedViews", wrapAdmin(h.PostScopedView))
	router.PUT("/config/rbac/scopedViews/:id", wrapAdmin(h.PutScopedView))
	router.DELETE("/config/rbac/scopedViews/:id", wrapAdmin(h.DeleteScopedView))
	router.GET("/config/rbac/scopedViews", wrapAdmin(h.GetScopedViews))
	router.GET("/config/rbac/scopedViews/:id", wrapAdmin(h.GetScopedViews))

	router.POST("/config/rbac/users/:userId/scopedViews", h.PostUserScopedView)
	router.GET("/config/rbac/policy/users/:userId", h.GetUserPolicy)
}
