package configrbac

import (
	"errors"
	"net/http"
	"os"
	"strings"
)

var (
	errUnauthorized = errors.New("unauthorized")
	errForbidden    = errors.New("forbidden")
)

// UserSubjectVerifier returns the authenticated Clerk user id for a request.
type UserSubjectVerifier interface {
	SubjectFromRequest(r *http.Request) (string, error)
}

// UserAuthInfo contains authenticated Clerk identity and organization role claims.
type UserAuthInfo struct {
	Subject string
	OrgRole string
}

type userAuthInfoVerifier interface {
	AuthInfoFromRequest(r *http.Request) (UserAuthInfo, error)
}

const clerkOrgRoleAdminEnvVar = "CLERK_ORG_ROLE_ADMIN"

func clerkOrgRoleAdmin() string {
	if role := strings.TrimSpace(os.Getenv(clerkOrgRoleAdminEnvVar)); role != "" {
		return role
	}
	return "org:admin"
}

func (info UserAuthInfo) IsOrgAdmin() bool {
	return strings.TrimSpace(info.OrgRole) == clerkOrgRoleAdmin()
}

func bearerToken(r *http.Request) (string, error) {
	const prefix = "Bearer "
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, prefix) {
		return "", errUnauthorized
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	if token == "" {
		return "", errUnauthorized
	}
	return token, nil
}
