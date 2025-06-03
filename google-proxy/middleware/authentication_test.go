package middleware

import (
	"bytes"
	"context"
	"crypto/rsa"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	servergenModel "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	utilsmiddleware "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type testProducer struct {
	writer io.Writer
	data   interface{}
	err    error
}

func (prod *testProducer) Produce(writer io.Writer, data interface{}) error {
	prod.writer = writer
	prod.data = data
	return prod.err
}

func TestAuthenticatedGCP(t *testing.T) {
	var jwtToken *jwt.Token
	var jwtErr error
	jwtParseWithClaimsMock := func(tokenString string, claims jwt.Claims, keyFunc jwt.Keyfunc, options ...jwt.ParserOption) (*jwt.Token, error) {
		return jwtToken, jwtErr
	}
	jwtParseWithClaims = jwtParseWithClaimsMock
	defer func() { jwtParseWithClaims = jwt.ParseWithClaims }()

	req := &http.Request{}
	req.Header = make(http.Header)
	req.Header.Set("Authorization", "")
	mockLogger := log.NewLogger()
	req = req.WithContext(context.WithValue(req.Context(), utilsmiddleware.ContextSLoggerKey, mockLogger))

	t.Run("WhenParsingJWTWithClaimsReturnsError", func(tt *testing.T) {
		jwtErr = errors.New("something went wrong")
		responder := AuthenticatedGCP(req, nil)
		if responder == nil {
			tt.Error("Responder not returned")
		} else {
			if authResponder, ok := responder.(*authenticationResponderGCP); !ok {
				tt.Error("Responder type does not match expected one")
			} else {
				if authResponder.code != http.StatusUnauthorized {
					tt.Error("Wrong error code in responder")
				}
			}
		}
	})
	t.Run("WhenParsingJWTWithClaimsReturnsValidationError", func(tt *testing.T) {
		jwtErr = &jwt.ValidationError{Errors: jwt.ValidationErrorMalformed, Inner: errors.New("malformed token")}
		responder := AuthenticatedGCP(req, nil)
		if responder == nil {
			tt.Error("Responder not returned")
		} else {
			if authResponder, ok := responder.(*authenticationResponderGCP); !ok {
				tt.Error("Responder type does not match expected one")
			} else {
				if authResponder.code != http.StatusUnauthorized {
					tt.Error("Wrong error code in responder")
				}
			}
		}
	})
	t.Run("WhenParsingJWTWithClaimsReturnsNilToken", func(tt *testing.T) {
		jwtToken = nil
		responder := AuthenticatedGCP(req, nil)
		if responder == nil {
			tt.Error("Responder not returned")
		} else {
			if authResponder, ok := responder.(*authenticationResponderGCP); !ok {
				tt.Error("Responder type does not match expected one")
			} else {
				if authResponder.code != http.StatusUnauthorized {
					tt.Error("Wrong error code in responder")
				}
			}
		}
	})
	t.Run("WhenParsingJWTWithClaimsReturnsInvalidToken", func(tt *testing.T) {
		jwtToken = &jwt.Token{}
		responder := AuthenticatedGCP(req, nil)
		if responder == nil {
			tt.Error("Responder not returned")
		} else {
			if authResponder, ok := responder.(*authenticationResponderGCP); !ok {
				tt.Error("Responder type does not match expected one")
			} else {
				if authResponder.code != http.StatusUnauthorized {
					tt.Error("Wrong error code in responder")
				}
			}
		}
	})
	t.Run("WhenParsingJWTWithClaimsReturnsTokenWithMissingClaims", func(tt *testing.T) {
		jwtToken = &jwt.Token{Valid: true}
		responder := AuthenticatedGCP(req, nil)
		if responder == nil {
			tt.Error("Responder not returned")
		} else {
			if authResponder, ok := responder.(*authenticationResponderGCP); !ok {
				tt.Error("Responder type does not match expected one")
			} else {
				if authResponder.code != http.StatusUnauthorized {
					tt.Error("Wrong error code in responder")
				}
			}
		}
	})
	t.Run("WhenProjectNumberInURLDoesn'tMatchToken", func(tt *testing.T) {
		req.URL = &url.URL{
			Host: "localhost:8000",
			Path: "/v1beta/projects/123/locations/:locationId/operations/:operationId",
		}
		jwtToken = &jwt.Token{Valid: true, Claims: &googleClaims{
			Google: &google{ConsumerProjectNumber: 123456},
		}}
		responder := AuthenticatedGCP(req, nil)
		if responder == nil {
			tt.Error("Responder not returned")
		} else {
			if authResponder, ok := responder.(*authenticationResponderGCP); !ok {
				tt.Error("Responder type does not match expected one")
			} else {
				if authResponder.code != http.StatusUnauthorized {
					tt.Error("Wrong error code in responder")
				}
			}
		}
	})
}

func Test_authenticationResponderGCP_WriteResponse(t *testing.T) {
	t.Run("WhenCallToProduceReturnsError", func(tt *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				tt.Log("Recovered from panic")
			}
		}()
		responder := &authenticationResponderGCP{message: "something went wrong"}
		rec := httptest.NewRecorder()
		prod := &testProducer{err: errors.New("failed to produce")}
		responder.WriteResponse(rec, prod)
		tt.Fail()
	})

	responder := &authenticationResponderGCP{code: 403, message: "something went wrong"}
	rec := httptest.NewRecorder()
	prod := &testProducer{}
	responder.WriteResponse(rec, prod)
	if rec.Code != http.StatusForbidden {
		t.Errorf("Response code %v does not match expected one", rec.Code)
	}
	if prod.writer != rec {
		t.Error("Writer passed into producer does not match expected one")
	}
	if prod.data == nil {
		t.Error("No data passed into producer")
	} else {
		if modelsError, ok := prod.data.(servergenModel.Error); !ok {
			t.Error("Type of data passed into producer does not match expected one")
		} else {
			code := float64(http.StatusForbidden)
			message := "something went wrong"
			expectedModelsError := servergenModel.Error{Code: code, Message: message}
			if !reflect.DeepEqual(modelsError, expectedModelsError) {
				t.Error("Data passed into producer does not match expected one")
			}
		}
	}
}

func TestJWTKeyFunc(t *testing.T) {
	var certificateMap map[string]string
	var certificateErr error
	fetchGoogleCertificates = func(string, string) (map[string]string, error) {
		return certificateMap, certificateErr
	}
	t.Run("WhenTokenHeaderHasNoKid", func(tt *testing.T) {
		key, err := jwtKeyFunc(&jwt.Token{})
		if err == nil {
			tt.Error("Error not returned")
		}
		if key != nil {
			tt.Error("Key unexpectedly returned")
		}
	})
	t.Run("WhenTokenHeaderHasInvalidKid", func(tt *testing.T) {
		token := &jwt.Token{
			Header: map[string]interface{}{"kid": false},
		}
		key, err := jwtKeyFunc(token)
		if err == nil {
			tt.Error("Error not returned")
		}
		if key != nil {
			tt.Error("Key unexpectedly returned")
		}
	})
	t.Run("WhenTokenSigningMethodIsWrong", func(tt *testing.T) {
		token := &jwt.Token{
			Header: map[string]interface{}{"kid": "billy"},
			Method: jwt.SigningMethodHS256,
		}
		key, err := jwtKeyFunc(token)
		if err == nil {
			tt.Error("Error not returned")
		}
		if key != nil {
			tt.Error("Key unexpectedly returned")
		}
	})
	t.Run("WhenTokenClaimsAreMissing", func(tt *testing.T) {
		token := &jwt.Token{
			Header: map[string]interface{}{"kid": "billy"},
			Method: &jwt.SigningMethodRSA{},
		}
		key, err := jwtKeyFunc(token)
		if err == nil {
			tt.Error("Error not returned")
		}
		if key != nil {
			tt.Error("Key unexpectedly returned")
		}
	})
	t.Run("WhenTokenClaimsAreInvalid", func(tt *testing.T) {
		token := &jwt.Token{
			Header: map[string]interface{}{"kid": "billy"},
			Method: &jwt.SigningMethodRSA{},
			Claims: &jwt.RegisteredClaims{},
		}
		key, err := jwtKeyFunc(token)
		if err == nil {
			tt.Error("Error not returned")
		}
		if key != nil {
			tt.Error("Key unexpectedly returned")
		}
	})
	t.Run("WhenTokenClaimsDoNotHaveClaimField", func(tt *testing.T) {
		token := &jwt.Token{
			Header: map[string]interface{}{"kid": "billy"},
			Method: &jwt.SigningMethodRSA{},
			Claims: &googleClaims{},
		}
		key, err := jwtKeyFunc(token)
		if err == nil {
			tt.Error("Error not returned")
		}
		if key != nil {
			tt.Error("Key unexpectedly returned")
		}
	})
	t.Run("WhenGoogleFieldDoesNotHavePermissions", func(tt *testing.T) {
		token := &jwt.Token{
			Header: map[string]interface{}{"kid": "billy"},
			Method: &jwt.SigningMethodRSA{},
			Claims: &googleClaims{Google: &google{}},
		}
		key, err := jwtKeyFunc(token)
		if err == nil {
			tt.Error("Error not returned")
		}
		if key != nil {
			tt.Error("Key unexpectedly returned")
		}
	})
	t.Run("WhenFetchingGoogleCertificatesReturnsError", func(tt *testing.T) {
		certificateErr = errors.New("something went wrong")
		token := &jwt.Token{
			Header: map[string]interface{}{"kid": "billy"},
			Method: &jwt.SigningMethodRSA{},
			Claims: &googleClaims{Google: &google{
				ConsumerProjectNumber: 323,
			}},
		}
		key, err := jwtKeyFunc(token)
		if err == nil {
			tt.Error("Error not returned")
		}
		if key != nil {
			tt.Error("Key unexpectedly returned")
		}
		certificateErr = nil
	})
	t.Run("WhenGoogleCertificateIsNotFound", func(tt *testing.T) {
		certificateMap = map[string]string{"wild-bill": "certified cowboy"}
		token := &jwt.Token{
			Header: map[string]interface{}{"kid": "billy"},
			Method: &jwt.SigningMethodRSA{},
			Claims: &googleClaims{Google: &google{
				ConsumerProjectNumber: 323,
			}}}
		key, err := jwtKeyFunc(token)
		if err == nil {
			tt.Error("Error not returned")
		}
		if key != nil {
			tt.Error("Key unexpectedly returned")
		}
		certificateMap = nil
	})
	t.Run("WhenIssuerIsWrong", func(tt *testing.T) {
		gcpAcceptedServiceAccounts = "ttt,sss"
		defer func() {
			gcpAcceptedServiceAccounts = ""
		}()
		jwtParseRSAPublicKeyFromPEM = func(key []byte) (*rsa.PublicKey, error) {
			return &rsa.PublicKey{}, nil
		}
		certificateMap = map[string]string{"billy": "THE kid"}
		token := &jwt.Token{
			Header: map[string]interface{}{"kid": "billy"},
			Method: &jwt.SigningMethodRSA{},
			Claims: &googleClaims{
				RegisteredClaims: jwt.RegisteredClaims{
					Audience: []string{"gcpURL"},
					Issuer:   "gcpSA",
				},
				Google: &google{
					ConsumerProjectNumber: 323,
				}}}
		key, err := jwtKeyFunc(token)
		require.EqualError(tt, err, "invalid issuer or audience in JWT")
		require.Nil(tt, key)
		certificateMap = nil
		jwtParseRSAPublicKeyFromPEM = jwt.ParseRSAPublicKeyFromPEM
	})
	t.Run("WhenGoogleSuccessful", func(tt *testing.T) {
		validateIssuerAndAudience = func(claims googleClaims) bool {
			return true
		}
		defer func() {
			validateIssuerAndAudience = _validateIssuerAndAudience
		}()
		jwtParseRSAPublicKeyFromPEM = func(key []byte) (*rsa.PublicKey, error) {
			return &rsa.PublicKey{}, nil
		}
		certificateMap = map[string]string{"billy": "THE kid"}
		token := &jwt.Token{
			Header: map[string]interface{}{"kid": "billy"},
			Method: &jwt.SigningMethodRSA{},
			Claims: &googleClaims{
				RegisteredClaims: jwt.RegisteredClaims{
					Audience: []string{"gcpURL"},
					Issuer:   "gcpSA",
				},
				Google: &google{
					ConsumerProjectNumber: 323,
				}}}
		key, err := jwtKeyFunc(token)
		if err != nil {
			tt.Error("Error unexpectedly returned - " + err.Error())
		}
		if key == nil {
			tt.Error("Key not returned")
		}
		certificateMap = nil
		jwtParseRSAPublicKeyFromPEM = jwt.ParseRSAPublicKeyFromPEM
	})
	fetchGoogleCertificates = _fetchGoogleCertificates
}

func TestFetchCerts(t *testing.T) {
	var jwk []byte
	var jwkErr error
	fetchJwk = func(jwkurl string) ([]byte, error) {
		return jwk, jwkErr
	}
	t.Run("WhenFetchingJWKReturnsError", func(tt *testing.T) {
		jwkErr = errors.New("something went wrong")
		jwkSleepRetryInterval = time.Duration(1) * time.Millisecond
		defer func() {
			jwkSleepRetryInterval = time.Duration(2) * time.Second
		}()
		certs, err := _fetchGoogleCertificates("test", "kid-rock")
		if err == nil {
			tt.Error("Error not returned")
		}
		if certs != nil {
			tt.Error("Certificates unexpectedly returned")
		}
		jwkErr = nil
	})
	t.Run("WhenUnmarshallingJWKReturnsError", func(tt *testing.T) {
		jwk = nil
		certs, err := _fetchGoogleCertificates("test", "kid-rock")
		if err == nil {
			tt.Error("Error not returned")
		}
		if certs != nil {
			tt.Error("Certificates unexpectedly returned")
		}
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		jwkString := `{"kid-rock":"bleh","billy-the-kid":"not-wild-bill"}`
		jwk = []byte(jwkString)
		certs, err := _fetchGoogleCertificates("test", "kid-rock")
		if err != nil {
			tt.Error("Error unexpectedly returned")
		}
		if certs == nil {
			tt.Error("Certificates not returned")
		} else {
			if len(certs) != 2 {
				tt.Errorf("Certificate count %v does not match expected one", len(certs))
			}
		}
		jwk = nil
	})
	t.Run("WhenSuccessfulUsingCache", func(tt *testing.T) {
		certificatesCache = map[string]*certCache{"kid-rock": {time.Now(), "bleh"}}
		certs, err := _fetchGoogleCertificates("test", "kid-rock")
		if err != nil {
			tt.Error("Error unexpectedly returned")
		}
		if certs == nil {
			tt.Error("Certificates not returned")
		} else {
			if len(certs) != 1 {
				tt.Errorf("Certificate count %v does not match expected one", len(certs))
			}
		}
	})
	t.Run("WhenSuccessfulAfterRetryFetchJwk", func(tt *testing.T) {
		jwkErr = errors.New("something went wrong")
		jwkString := `{"kid-rock":"bleh","billy-the-kid":"not-wild-bill"}`
		jwkSleepRetryInterval = time.Duration(10) * time.Millisecond
		defer func() {
			jwkSleepRetryInterval = time.Duration(2) * time.Second
		}()
		go func() {
			time.Sleep(20 * time.Millisecond)
			jwkErr = nil
			jwk = []byte(jwkString)
		}()
		certs, err := _fetchGoogleCertificates("test", "kid-rock")
		if err != nil {
			tt.Error("Error unexpectedly returned")
		}
		if certs == nil {
			tt.Error("Certificates not returned")
		}
		jwk = nil
	})
	fetchJwk = _fetchJwk
	certificatesCache = map[string]*certCache{}
}

func TestCleanpCertCache(t *testing.T) {
	t.Run("ClearCertCacheOnError", func(tt *testing.T) {
		certificatesCache = map[string]*certCache{"kid-rock": {time.Now(), "bleh"}}
		cleanupCertCache()
		assert.Equal(tt, 0, len(certificatesCache))
	})
}

func TestFetchJWK(t *testing.T) {
	var httpResp *http.Response
	var httpErr error
	jwkClientGet = func(url string) (*http.Response, error) {
		return httpResp, httpErr
	}
	t.Run("WhenHTTPGetReturnsError", func(tt *testing.T) {
		// Arrange
		httpErr = errors.New("something went wrong")

		// Act
		jwk, err := fetchJwk("")

		// Assert
		assert.Exactly(tt, httpErr, err)
		assert.Nil(tt, jwk)
		httpErr = nil
	})
	t.Run("WhenReadingResponseBodyReturnsError", func(tt *testing.T) {
		// Arrange
		httpResp = httptest.NewRecorder().Result()

		mockError := errors.New("mockError")
		mockBody := &bodyReadCloser{readError: mockError}
		httpResp.Body = mockBody

		// Act
		jwk, err := fetchJwk("")

		// Assert
		assert.Exactly(tt, mockError, err)
		assert.Empty(tt, jwk)
		assert.True(tt, mockBody.closed)
		n, _ := httpResp.Body.Read(make([]byte, 1))
		assert.Zero(tt, n, "Response Body should be read til the end, so the Transport can be reused by Go")
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		// Arrange
		httpResp = httptest.NewRecorder().Result()

		mockJWK := []byte("JWK")
		mockBody := &bodyReadCloser{body: bytes.NewBuffer(mockJWK)}
		httpResp.Body = mockBody

		// Act
		jwk, err := fetchJwk("")

		// Assert
		assert.NoError(tt, err)
		assert.Equal(tt, mockJWK, jwk)
		assert.True(tt, mockBody.closed)
		n, _ := httpResp.Body.Read(make([]byte, 1))
		assert.Zero(tt, n, "Response Body should be read til the end, so the Transport can be reused by Go")
	})
	jwkClientGet = _jwkClientGet
}

func TestValidateProjectNumber(t *testing.T) {
	req := &http.Request{}
	req.Header = make(http.Header)
	req.Header.Set("Authorization", "")
	req.URL = &url.URL{
		Host: "localhost:8000",
		Path: "/v1beta/projects/123/locations/:locationId/operations/:operationId",
	}
	t.Run("WhenValidateProjectNumberFails", func(tt *testing.T) {
		err := validateProjectNumber(req, "1234")
		if err == nil {
			tt.Error("Error expected")
		}
	})
	t.Run("WhenValidateProjectNumberSucceeds", func(tt *testing.T) {
		err := validateProjectNumber(req, "123")
		if err != nil {
			tt.Error("Error unexpectedly returned")
		}
	})
}

func TestAuthMiddleware_BypassForHealthAndMetrics(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	tests := []struct {
		path string
	}{
		{path: "/health"},
		{path: "/metrics"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			called = false
			req := httptest.NewRequest("GET", tt.path, nil)
			rr := httptest.NewRecorder()
			handler := AuthMiddleware(next)
			handler.ServeHTTP(rr, req)
			assert.True(t, called, "Handler should be called for %s", tt.path)
			assert.Equal(t, http.StatusOK, rr.Code)
		})
	}
}

func TestAuthMiddleware_DoesNotBypassForOtherRoutes(t *testing.T) {
	paths := []string{"/api", "/v1/resource", "/random"}
	for _, path := range paths {
		t.Run(path, func(tt *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					tt.Errorf("Expected panic for %s but none occurred", path)
				}
			}()
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})
			req := httptest.NewRequest("GET", path, nil)
			rr := httptest.NewRecorder()
			handler := AuthMiddleware(next)
			handler.ServeHTTP(rr, req)
			tt.Errorf("Handler should not return normally for %s", path)
		})
	}
}

type bodyReadCloser struct {
	body      io.Reader
	closed    bool
	readError error
}

func (body *bodyReadCloser) Read(p []byte) (n int, err error) {
	if body.readError != nil {
		return 0, body.readError
	}
	return body.body.Read(p)
}

func (body *bodyReadCloser) Close() error {
	body.closed = true
	return nil
}
