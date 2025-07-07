package activities

import (
	"context"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type CommonActivities struct {
	SE database.Storage
}

var (
	GetProviderByNode = _getProviderByNode
	getSignedJwtToken = auth.GetSignedJwtToken
)

func (ca CommonActivities) CreateJob(ctx context.Context, job *datamodel.Job) (*datamodel.Job, error) {
	logger := util.GetLogger(ctx)
	se := ca.SE
	logger.Infof("creating job: %s with status: %s", job.UUID, job.State)
	return se.CreateJob(ctx, job)
}

// UpdateJobStatus updates the status of a job in the database.
func (ca CommonActivities) UpdateJobStatus(ctx context.Context, job *datamodel.Job) error {
	logger := util.GetLogger(ctx)
	se := ca.SE
	logger.Infof("updating job: %s with status: %s", job.UUID, job.State)
	return se.UpdateJob(ctx, job.UUID, job.State, job.TrackingID, job.ErrorDetails)
}

// DescribeJob gives the status of a job in the database.
func DescribeJob(ctx context.Context, jobId, basepath, jwtToken, projectNumber, location *string) error {
	if jobId == nil {
		return nil
	}
	logger := util.GetLogger(ctx)
	googleProxyClient := googleproxyclient.GetGProxyClient(*basepath, *jwtToken, logger)

	describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
		OperationId:   *jobId,
		ProjectNumber: *projectNumber,
		LocationId:    *location,
	}

	res, err := googleProxyClient.Invoker.V1betaDescribeOperation(ctx, describeOperationParams)
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDescribingJob, err)
	}
	operation, ok := res.(*googleproxyclient.OperationV1beta)
	if ok {
		if operation.Done.Value {
			return nil
		}
	}
	return vsaerrors.NewVCPError(vsaerrors.ErrJobNotFinished, errors.New("job not finished"))
}

// GetNode retrieves the node associated with the given pool ID.
func (ca CommonActivities) GetNode(ctx context.Context, poolId int64) ([]*datamodel.Node, error) {
	se := ca.SE

	nodes, err := se.GetNodesByPoolID(ctx, poolId)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, errors.New("no node found for the pool")
	}

	return nodes, nil
}

func _getProviderByNode(ctx context.Context, node *models.Node) (vsa.Provider, error) {
	var password string
	if common.AuthType == common.USERNAME_PWD_SEC_MGR {
		password = GetPasswordFromCacheOrSecretManager(ctx, node.SecretID)
	} else {
		password = node.Password
	}

	// if ipAddress in empty, populate it with the node's endpoint address
	if len(node.EndpointAddresses) == 0 {
		if node.EndpointAddress == "" {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrVSAClusterNodeIPAddressNotFound, errors.New("node endpoint address is empty"))
		}
		node.EndpointAddresses = []string{node.EndpointAddress}
	}

	return vsa.NewProvider(vsa.ProviderDetails{
		IPAddresses: node.EndpointAddresses,
		UserName:    node.Username,
		Password:    password,
		// TODO : need to fix once we have certs
		InsecureSkipVerify: true,
	}), nil
}

func (j CommonActivities) GetOntapJob(ctx context.Context, jobUUID string, node *models.Node) (*vsa.OntapJob, error) {
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	job, err := provider.JobGet(jobUUID)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (j CommonActivities) GetAuthJWTToken(ctx context.Context, accountName string) (string, error) {
	logger := util.GetLogger(ctx)
	jwtToken, err := getSignedJwtToken(accountName)
	if err != nil {
		logger.Errorf("failed to get token for account %s: %v", accountName, err)
		return "", errors.New("failed to get token")
	}
	return jwtToken, nil
}
