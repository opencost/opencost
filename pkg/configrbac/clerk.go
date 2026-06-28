package configrbac

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const clerkIssuerEnvVar = "CLERK_ISSUER"

type clerkClaims struct {
	jwt.RegisteredClaims
	OrgRole string `json:"org_role"`
}

// ClerkJWTVerifier validates Clerk session JWTs and returns identity claims.
type ClerkJWTVerifier struct {
	mu     sync.Mutex
	keys   map[string]*rsa.PublicKey
	issuer string
}

func NewClerkJWTVerifier() *ClerkJWTVerifier {
	return &ClerkJWTVerifier{issuer: strings.TrimRight(os.Getenv(clerkIssuerEnvVar), "/")}
}

func (v *ClerkJWTVerifier) SubjectFromRequest(r *http.Request) (string, error) {
	info, err := v.AuthInfoFromRequest(r)
	if err != nil {
		return "", err
	}
	return info.Subject, nil
}

func (v *ClerkJWTVerifier) AuthInfoFromRequest(r *http.Request) (UserAuthInfo, error) {
	tokenString, err := bearerToken(r)
	if err != nil {
		return UserAuthInfo{}, err
	}
	claims := clerkClaims{}
	parser := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}))
	if v.issuer != "" {
		parser = jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}), jwt.WithIssuer(v.issuer))
	}
	token, err := parser.ParseWithClaims(tokenString, &claims, func(token *jwt.Token) (interface{}, error) {
		kid, _ := token.Header["kid"].(string)
		if kid == "" {
			return nil, errUnauthorized
		}
		issuer := v.issuer
		if issuer == "" {
			if claims.Issuer == "" {
				return nil, errUnauthorized
			}
			issuer = strings.TrimRight(claims.Issuer, "/")
			if !isDefaultClerkIssuer(issuer) {
				return nil, errUnauthorized
			}
		}
		return v.keyFor(issuer, kid)
	})
	if err != nil || !token.Valid {
		return UserAuthInfo{}, errUnauthorized
	}
	if claims.Subject == "" {
		return UserAuthInfo{}, errUnauthorized
	}
	return UserAuthInfo{Subject: claims.Subject, OrgRole: claims.OrgRole}, nil
}

func isDefaultClerkIssuer(issuer string) bool {
	parsed, err := url.Parse(issuer)
	if err != nil || parsed.Scheme != "https" {
		return false
	}
	return parsed.Host == "clerk.accounts.dev" || strings.HasSuffix(parsed.Host, ".clerk.accounts.dev")
}

func (v *ClerkJWTVerifier) keyFor(issuer, kid string) (*rsa.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.keys == nil {
		v.keys = map[string]*rsa.PublicKey{}
	}
	cacheKey := issuer + "\x00" + kid
	if key, ok := v.keys[cacheKey]; ok {
		return key, nil
	}
	key, err := fetchClerkJWK(issuer, kid)
	if err != nil {
		return nil, err
	}
	v.keys[cacheKey] = key
	return key, nil
}

type jwksResponse struct {
	Keys []struct {
		KID string `json:"kid"`
		KTY string `json:"kty"`
		N   string `json:"n"`
		E   string `json:"e"`
	} `json:"keys"`
}

func fetchClerkJWK(issuer, kid string) (*rsa.PublicKey, error) {
	parsed, err := url.Parse(issuer)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, errors.New("invalid clerk issuer")
	}
	jwksURL := strings.TrimRight(issuer, "/") + "/.well-known/jwks.json"
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(jwksURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jwks status %d", resp.StatusCode)
	}
	var jwks jwksResponse
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return nil, err
	}
	for _, key := range jwks.Keys {
		if key.KID != kid || key.KTY != "RSA" {
			continue
		}
		n, err := base64.RawURLEncoding.DecodeString(key.N)
		if err != nil {
			return nil, err
		}
		eBytes, err := base64.RawURLEncoding.DecodeString(key.E)
		if err != nil {
			return nil, err
		}
		e := new(big.Int).SetBytes(eBytes).Int64()
		return &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(e)}, nil
	}
	return nil, errors.New("jwks key not found")
}
