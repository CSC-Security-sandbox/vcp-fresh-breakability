package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	credentials2 "cloud.google.com/go/iam/credentials/apiv1/credentialspb"
	errs "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	// GetSignedJwtToken returns a signed JWT for GCP
	GetSignedJwtToken       = _getSignedJwtToken
	GetSignedAccessToken    = _getSignedAccessToken
	hydrationServiceAccount = env.GetString("GCP_HYDRATE_AUTH_SERVICE_ACCOUNT", "")
	LogGetLogger            = util.GetLogger
	parseInt                = strconv.ParseInt
	jsonMarshal             = json.Marshal
	timeNow                 = time.Now
	GenerateCallbackToken   = _generateCallbackToken
)

func _generateCallbackToken(ctx context.Context) (string, error) {
	logger := util.GetLogger(ctx)
	callbackToken, err := GetSignedAccessToken()
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return "", err
	}
	return callbackToken, nil
}

func _getSignedJwtToken(projectNumber string) (string, error) {
	if env.GetString("ENV", "") == "local" {
		return "token", nil
	}

	ctx := context.Background()
	timeAtStart := timeNow()
	ttl := time.Duration(env.GetInt("JWT_TTL_MINUTES", 60)) * time.Minute
	serviceAccount := env.GetString("GCP_AUTH_SERVICE_ACCOUNT", "")
	serviceUrl := env.GetString("GCP_SERVICE_URL", "")
	isIntegrationTest := env.GetString("INTEGRATION_TEST", "")
	nkdevTest := env.GetBool("NKDEV_TEST", false)
	logger := LogGetLogger(ctx)
	var c credentialsClientWrapper
	var err error

	if nkdevTest || isIntegrationTest == "true" {
		if mockAccessToken != "" {
			logger.Info(fmt.Sprintf("using mock access token: %s", mockAccessToken))
			return mockAccessToken, nil
		}
		c, err = createMockIamClient(ctx)
	} else {
		c, err = createIamClient(ctx)
	}

	if err != nil {
		logger.Error("Error when creating iam client", err)
		return "", errs.NewVCPError(errs.ErrFailedToCreateNewIamCred, err)
	}
	defer func(c credentialsClientWrapper) {
		err := c.Close()
		if err != nil {
			logger.Error("err", err)
		}
	}(c)

	projectNumberInt, err := parseInt(projectNumber, 10, 64)
	if err != nil {
		logger.Error("Failed to parse projectNumber")
		return "", errs.NewVCPError(errs.ErrFailedToParseProjectNumber, err)
	}

	payload := JwtPayload{
		Subject:    serviceAccount,
		Audience:   serviceUrl,
		Issuer:     serviceAccount,
		Expiration: timeAtStart.Add(ttl).Unix(),
		IssuedAt:   timeAtStart.Unix(),
		Google: Google{
			ProjectNumber: projectNumberInt,
		},
	}

	jsonPayload, err := jsonMarshal(payload)
	if err != nil {
		logger.Error("Failed to marshal jwt payload")
		return "", errs.NewVCPError(errs.ErrFailedToMarshalPayload, err)
	}

	reqToken := &credentials2.SignJwtRequest{
		Name:      "projects/-/serviceAccounts/" + serviceAccount,
		Delegates: []string{"projects/-/serviceAccounts/" + serviceAccount},
		Payload:   string(jsonPayload),
	}

	tt, err := c.SignJwt(ctx, reqToken)
	if err != nil {
		return "", errs.NewVCPError(errs.ErrFailedToGenerateAccessToken, err)
	}

	return tt.SignedJwt, nil
}

func _getSignedAccessToken() (string, error) {
	if env.GetString("ENV", "") == "local" {
		return "token", nil
	}

	ctx := context.Background()
	logger := LogGetLogger(ctx)
	c, err := createIamClient(ctx)
	if err != nil {
		return "", err
	}
	defer func(c credentialsClientWrapper) {
		err := c.Close()
		if err != nil {
			logger.Error("auth failed to close credentials client")
		}
	}(c)

	reqToken := &credentials2.GenerateAccessTokenRequest{
		Name:      "projects/-/serviceAccounts/" + hydrationServiceAccount,
		Delegates: []string{"projects/-/serviceAccounts/" + hydrationServiceAccount},
		Scope:     []string{"https://www.googleapis.com/auth/cloud-platform"},
		Lifetime:  nil,
	}

	tt, err := c.GenerateAccessToken(ctx, reqToken)
	if err != nil {
		return "", err
	}

	return tt.AccessToken, nil
}
