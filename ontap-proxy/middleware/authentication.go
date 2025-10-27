package middleware

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	jwkClient *http.Client
	mutex     sync.Mutex

	runningEnv                  = env.GetString("ENV", "")
	certificatesCache           = map[string]*certCache{}
	certCacheExpiration         = time.Duration(env.GetInt("CERT_CACHE_EXPIRATION_MINUTES", 120)) * time.Minute
	certCacheSleepRetryInterval = time.Duration(env.GetInt("CERT_CACHE_SLEEP_RETRY_INTERVAL_SECONDS", 5)) * time.Second
	jwkTimeout                  = time.Duration(env.GetInt("JWK_TIMEOUT_SECONDS", 10)) * time.Second
	gcpAcceptedServiceAccounts  = env.GetString("GCP_AUTH_ACCEPTED_SERVICE_ACCOUNTS", "")
	gcpServiceURL               = env.GetString("GCP_SERVICE_URL", "")
	jwkURL                      = env.GetString("JWK_KIDS_URL", "https://www.googleapis.com/service_accounts/v1/metadata/x509/")
	jwkRetryCount               = 5
	jwkSleepRetryInterval       = time.Duration(2) * time.Second
	certCacheMaxRetryCount      = 3
	retryErrors                 = []string{"crypto/rsa: verification error", "context deadline exceeded"}

	jwtParseWithClaims          = jwt.ParseWithClaims
	jwtParseRSAPublicKeyFromPEM = jwt.ParseRSAPublicKeyFromPEM
	fetchGoogleCertificates     = _fetchGoogleCertificates
	fetchJwk                    = _fetchJwk
	jwkClientGet                = _jwkClientGet
	validateIssuerAndAudience   = _validateIssuerAndAudience
)

type googleClaims struct {
	jwt.RegisteredClaims
	Google *google `json:"google,omitempty"`
}

type google struct {
	ConsumerProjectNumber int `json:"project_number"`
}

type certCache struct {
	time      time.Time
	publickey string
}

func AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger := util.GetLogger(r.Context())

		if err := AuthenticateGCP(r); err != nil {
			logger.ErrorContext(r.Context(), "Authentication failed", "error", err.Error(), "path", r.URL.Path)
			http.Error(w, "Authentication failed", http.StatusUnauthorized)
			return
		}

		logger.DebugContext(r.Context(), "Authentication successful", "path", r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

func AuthenticateGCP(req *http.Request) error {
	if runningEnv == "local" {
		return nil
	}

	authorizationHeader := req.Header.Get("authorization")
	if authorizationHeader == "" {
		return errors.New("missing authorization header")
	}

	tokenString := strings.TrimPrefix(authorizationHeader, "Bearer ")
	if tokenString == authorizationHeader {
		return errors.New("invalid authorization header format, expected 'Bearer <token>'")
	}

	token, err := jwtParseWithClaims(tokenString, &googleClaims{}, jwtKeyFunc)
	for retryCount := 1; retryCount <= certCacheMaxRetryCount && err != nil && shouldRetry(err, retryErrors); retryCount++ {
		time.Sleep(certCacheSleepRetryInterval)
		cleanupCertCache()
		token, err = jwtParseWithClaims(tokenString, &googleClaims{}, jwtKeyFunc)
	}
	if err != nil {
		return fmt.Errorf("JWT parsing failed: %w", err)
	}
	if token == nil || !token.Valid {
		return errors.New("invalid JWT token")
	}

	claims, ok := token.Claims.(*googleClaims)
	if !ok || claims == nil || claims.Google == nil {
		return errors.New("invalid JWT claims")
	}

	tokenProjectNumber := strconv.Itoa(claims.Google.ConsumerProjectNumber)
	if err := validateProjectNumber(req, tokenProjectNumber); err != nil {
		return fmt.Errorf("project validation failed: %w", err)
	}

	return nil
}

func jwtKeyFunc(token *jwt.Token) (interface{}, error) {
	var certificates map[string]string
	var err error

	kid := token.Header["kid"]
	kidString, ok := kid.(string)
	if !ok {
		return nil, errors.New("invalid kid field in JWT")
	}
	if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
		return nil, errors.New("invalid signing method on token")
	}
	claims, ok := token.Claims.(*googleClaims)
	if !ok {
		return nil, errors.New("claims missing from JWT")
	}
	if !validateIssuerAndAudience(*claims) {
		return nil, errors.New("invalid issuer or audience in JWT")
	}
	certificates, err = fetchGoogleCertificates(claims.Issuer, kidString)
	if err != nil {
		return nil, err
	}
	certificate, ok := certificates[kidString]
	if !ok {
		return nil, errors.New("Certificate for kid " + kidString + " not found at issuer")
	}
	key, err := jwtParseRSAPublicKeyFromPEM([]byte(certificate))
	if key == nil {
		return nil, err
	}

	return key, err
}

func _fetchGoogleCertificates(issuer string, keyString string) (certificates map[string]string, err error) {
	certificates, valid := getCertsFromCacheWithValidity()
	if valid && certificates != nil {
		if _, ok := certificates[keyString]; ok {
			return certificates, nil
		}
	}

	jwkKidURL := jwkURL + issuer
	jwk, err := fetchJwk(jwkKidURL)
	for retryCount := 1; retryCount <= jwkRetryCount && err != nil; retryCount++ {
		log.NewLogger().Info("Failed to fetch Google JWK, retrying", "error", err, "retryCount", retryCount)
		time.Sleep(jwkSleepRetryInterval)
		jwk, err = fetchJwk(jwkKidURL)
	}
	if err != nil {
		err = errors.New("unable to fetch Google JWK")
		return
	}
	err = json.Unmarshal(jwk, &certificates)
	if err != nil {
		err = errors.New("unable to parse Google JWK")
		return
	}
	updateCertCache(time.Now(), certificates)
	return
}

func _validateIssuerAndAudience(claims googleClaims) bool {
	lst := strings.Split(gcpAcceptedServiceAccounts, ",")
	if utils.ContainsString(lst, claims.Issuer) && claims.VerifyAudience(gcpServiceURL, true) {
		return true
	}
	return false
}

func _fetchJwk(jwkURL string) (jwk []byte, err error) {
	res, err := jwkClientGet(jwkURL)
	if err != nil {
		return
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.NewLogger().Error("Failed to close response body", "error", err)
		}
	}(res.Body)

	if res.StatusCode != http.StatusOK {
		err = errors.New("failed to fetch remote JWK (status != 200)")
		return
	}

	jwk, err = io.ReadAll(res.Body)
	return
}

func _jwkClientGet(url string) (*http.Response, error) {
	if jwkClient == nil {
		jwkClient = &http.Client{
			Transport: &http.Transport{
				TLSNextProto: map[string]func(authority string, c *tls.Conn) http.RoundTripper{},
				TLSClientConfig: &tls.Config{
					Renegotiation: tls.RenegotiateOnceAsClient,
				},
			},
			Timeout: jwkTimeout,
		}
	}
	return jwkClient.Get(url)
}

func getCertsFromCacheWithValidity() (map[string]string, bool) {
	mutex.Lock()
	defer mutex.Unlock()

	certificate := make(map[string]string)
	for key, value := range certificatesCache {
		if time.Since(value.time) < certCacheExpiration {
			certificate[key] = value.publickey
			return certificate, true
		}
	}
	return nil, false
}

func cleanupCertCache() {
	updateCertCache(time.Time{}, nil)
}

func updateCertCache(time time.Time, certificates map[string]string) {
	mutex.Lock()
	defer mutex.Unlock()

	if certificates == nil {
		for key := range certificatesCache {
			delete(certificatesCache, key)
		}
	}
	for key, value := range certificates {
		certificatesCache[key] = &certCache{time: time, publickey: value}
	}
}

func shouldRetry(err error, errs []string) bool {
	for _, e := range errs {
		if strings.Contains(err.Error(), e) {
			return true
		}
	}
	return false
}

func validateProjectNumber(req *http.Request, tokenProjectNumber string) error {
	pathParts := strings.Split(req.URL.Path, "/")
	if len(pathParts) < 4 || pathParts[1] != "v1beta" || pathParts[2] != "projects" {
		return errors.New("invalid URL path format")
	}

	urlProjectId := pathParts[3]

	if urlProjectId == "" {
		return errors.New("missing project ID in URL")
	}

	return nil
}
