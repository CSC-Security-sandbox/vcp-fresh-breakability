package auth

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	credentials2 "cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type monkeyMock struct {
	mock.Mock
}

func (_m *monkeyMock) Patch() {
	createIamClient = _m.createIamClient
	LogGetLogger = _m.LogGetLogger
	parseInt = _m.parseInt
	jsonMarshal = _m.jsonMarshal
	timeNow = _m.timeNow
}

func (_m *monkeyMock) UnPatch() {
	createIamClient = _createIamClient
	timeNow = time.Now
	LogGetLogger = util.GetLogger
	parseInt = strconv.ParseInt
	jsonMarshal = json.Marshal
}

func (_m *monkeyMock) timeNow() time.Time {
	ret := _m.Called()

	var r0 time.Time

	if ret.Get(0) != nil {
		r0 = ret.Get(0).(time.Time)
	}

	return r0
}

func (_m *monkeyMock) jsonMarshal(v interface{}) ([]byte, error) {
	ret := _m.Called(v)

	var r0 []byte
	var r1 error

	if ret.Get(0) != nil {
		r0 = ret.Get(0).([]byte)
	}
	if ret.Get(1) != nil {
		r1 = ret.Get(1).(error)
	}

	return r0, r1
}

func (_m *monkeyMock) parseInt(s string, base int, bitSize int) (i int64, err error) {
	ret := _m.Called(s, base, bitSize)

	var r0 int64
	var r1 error

	if ret.Get(0) != nil {
		r0 = ret.Get(0).(int64)
	}
	if ret.Get(1) != nil {
		r1 = ret.Get(1).(error)
	}

	return r0, r1
}

func (_m *monkeyMock) createIamClient(ctx context.Context) (credentialsClientWrapper, error) {
	ret := _m.Called(ctx)

	var r0 credentialsClientWrapper
	var r1 error

	if ret.Get(0) != nil {
		r0 = ret.Get(0).(credentialsClientWrapper)
	}
	if ret.Get(1) != nil {
		r1 = ret.Get(1).(error)
	}

	return r0, r1
}

type credentialsClientWrapperMock struct {
	mock.Mock
}

func (_c *credentialsClientWrapperMock) SignJwt(ctx context.Context, req *credentials2.SignJwtRequest, opts ...gax.CallOption) (*credentials2.SignJwtResponse, error) {
	ret := _c.Called(ctx, req)

	var r0 *credentials2.SignJwtResponse
	var r1 error

	if ret.Get(0) != nil {
		r0 = ret.Get(0).(*credentials2.SignJwtResponse)
	}
	if ret.Get(1) != nil {
		r1 = ret.Get(1).(error)
	}

	return r0, r1
}

func (_c *credentialsClientWrapperMock) Close() error {
	ret := _c.Called()

	var r0 error

	if ret.Get(0) != nil {
		r0 = ret.Get(0).(error)
	}
	return r0
}

func (_c *credentialsClientWrapperMock) GenerateAccessToken(ctx context.Context, req *credentials2.GenerateAccessTokenRequest, opts ...gax.CallOption) (*credentials2.GenerateAccessTokenResponse, error) {
	ret := _c.Called(ctx, req, opts)

	var r0 *credentials2.GenerateAccessTokenResponse
	var r1 error

	if ret.Get(0) != nil {
		r0 = ret.Get(0).(*credentials2.GenerateAccessTokenResponse)
	}
	if ret.Get(1) != nil {
		r1 = ret.Get(1).(error)
	}
	return r0, r1
}

func (_m *monkeyMock) LogGetLogger(ctx interface{}) log.Logger {
	ret := _m.Called(ctx)

	var r0 log.Logger

	if ret.Get(0) != nil {
		r0 = ret.Get(0).(log.Logger)
	}
	return r0
}
