package transport

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestNewAuthenticationRoundTripper(t *testing.T) {
	mrt := NewMockRoundTripper(t)
	prt := NewAuthenticationRoundTripper(mrt, common.Admin, "password", true)

	assert.IsType(t, &AuthenticationRoundTripper{}, prt)
	assert.Equal(t, mrt, prt.roundTripper)
	assert.Equal(t, true, prt.useCert)
	assert.Equal(t, common.Admin, prt.username)
	assert.Equal(t, "password", string(prt.password))
}

func TestAuthenticationRoundTripperRoundTrip(t *testing.T) {
	t.Run("WhenUsingOAuthFails", func(tt *testing.T) {
		ontapRestOAuthEnabledOld := ontapRestOAuthEnabled
		ontapRestOAuthEnabled = true
		defer func() {
			ontapRestOAuthEnabled = ontapRestOAuthEnabledOld
		}()
		mrt := NewMockRoundTripper(t)
		art := &AuthenticationRoundTripper{roundTripper: mrt}
		req := &http.Request{
			Header: map[string][]string{},
		}
		eErr := errors.New("expected")
		mrt.On("RoundTrip", mock.Anything).Return(nil, eErr)

		res, err := art.RoundTrip(req)
		assert.Nil(tt, res)
		assert.Equal(tt, eErr, err)
		assert.Equal(tt, "Bearer ", req.Header.Get("Authorization"))

		mrt.AssertCalled(tt, "RoundTrip", req)
		mrt.AssertExpectations(tt)
	})

	t.Run("WhenUsingOAuthSucceeds", func(tt *testing.T) {
		ontapRestOAuthEnabledOld := ontapRestOAuthEnabled
		token = "**"
		ontapRestOAuthEnabled = true
		defer func() {
			ontapRestOAuthEnabled = ontapRestOAuthEnabledOld
			token = ""
		}()
		mrt := NewMockRoundTripper(t)
		art := &AuthenticationRoundTripper{roundTripper: mrt}
		req := &http.Request{
			Header: map[string][]string{},
		}
		resp := &http.Response{}
		mrt.On("RoundTrip", mock.Anything).Return(resp, nil)

		res, err := art.RoundTrip(req)
		assert.Nil(tt, err)
		assert.Same(tt, resp, res)
		assert.Equal(tt, "Bearer **", req.Header.Get("Authorization"))
		mrt.AssertCalled(tt, "RoundTrip", req)
		mrt.AssertExpectations(tt)
	})

	t.Run("WhenUsingCertFails", func(tt *testing.T) {
		ontapRestOAuthEnabledOld := ontapRestOAuthEnabled
		ontapRestOAuthEnabled = false
		defer func() {
			ontapRestOAuthEnabled = ontapRestOAuthEnabledOld
		}()
		mrt := NewMockRoundTripper(t)
		art := &AuthenticationRoundTripper{roundTripper: mrt, useCert: true}
		req := &http.Request{
			Header: map[string][]string{},
		}
		eErr := errors.New("expected")
		mrt.On("RoundTrip", mock.Anything).Return(nil, eErr)

		res, err := art.RoundTrip(req)
		assert.Nil(tt, res)
		assert.Equal(tt, eErr, err)
		assert.Empty(tt, req.Header.Get("Authorization"))
		mrt.AssertCalled(tt, "RoundTrip", req)
		mrt.AssertExpectations(tt)
	})

	t.Run("WhenUsingCertSucceeds", func(tt *testing.T) {
		ontapRestOAuthEnabledOld := ontapRestOAuthEnabled
		ontapRestOAuthEnabled = false
		defer func() {
			ontapRestOAuthEnabled = ontapRestOAuthEnabledOld
		}()
		mrt := NewMockRoundTripper(t)
		art := &AuthenticationRoundTripper{roundTripper: mrt, useCert: true}
		req := &http.Request{
			Header: map[string][]string{},
		}
		resp := &http.Response{}
		mrt.On("RoundTrip", mock.Anything).Return(resp, nil)

		res, err := art.RoundTrip(req)
		assert.Nil(tt, err)
		assert.Same(tt, resp, res)
		assert.Empty(tt, req.Header.Get("Authorization"))
		mrt.AssertCalled(tt, "RoundTrip", req)
		mrt.AssertExpectations(tt)
	})

	t.Run("WhenUsingBasicFails", func(tt *testing.T) {
		ontapRestOAuthEnabledOld := ontapRestOAuthEnabled
		ontapRestOAuthEnabled = false
		defer func() {
			ontapRestOAuthEnabled = ontapRestOAuthEnabledOld
		}()
		mrt := NewMockRoundTripper(t)
		art := &AuthenticationRoundTripper{roundTripper: mrt, username: "user", password: log.Secret("no")}
		req := &http.Request{
			Header: map[string][]string{},
		}
		eErr := errors.New("expected")
		mrt.On("RoundTrip", mock.Anything).Return(nil, eErr)

		res, err := art.RoundTrip(req)
		assert.Nil(tt, res)
		assert.Equal(tt, eErr, err)
		assert.True(tt, strings.Contains(req.Header.Get("Authorization"), "Basic"))
		mrt.AssertCalled(tt, "RoundTrip", req)
		mrt.AssertExpectations(tt)
	})

	t.Run("WhenUsingBasicSucceeds", func(tt *testing.T) {
		ontapRestOAuthEnabledOld := ontapRestOAuthEnabled
		ontapRestOAuthEnabled = false
		defer func() {
			ontapRestOAuthEnabled = ontapRestOAuthEnabledOld
		}()

		mrt := NewMockRoundTripper(t)
		art := &AuthenticationRoundTripper{roundTripper: mrt}
		req := &http.Request{
			Header: map[string][]string{},
		}
		resp := &http.Response{}
		mrt.On("RoundTrip", mock.Anything).Return(resp, nil)

		res, err := art.RoundTrip(req)
		assert.Nil(tt, err)
		assert.Same(tt, resp, res)
		assert.True(tt, strings.Contains(req.Header.Get("Authorization"), "Basic"))
		mrt.AssertCalled(tt, "RoundTrip", req)
		mrt.AssertExpectations(tt)
	})
}
