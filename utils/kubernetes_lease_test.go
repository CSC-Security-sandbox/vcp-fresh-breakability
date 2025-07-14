package utils

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"

	"k8s.io/client-go/kubernetes/fake"
	v1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
)

func TestCreateKubernetesLease_Success(t *testing.T) {
	testNameSpace := "TestNameSpace"
	ctx := context.Background()
	oldLeaseClient := getLeaseClient
	defer func() { getLeaseClient = oldLeaseClient }()
	fakeCS := fake.NewClientset()
	getLeaseClient = func(leaseNameSpace string) (v1.LeaseInterface, error) {
		leaseClient := fakeCS.CoordinationV1().Leases(leaseNameSpace)
		return leaseClient, nil
	}
	err := CreateKubernetesLease(ctx, testNameSpace, "testNameSpace")
	assert.Nil(t, err)
}

func TestCreateKubernetesLease_ClientError(t *testing.T) {
	testNameSpace := "TestNameSpace"
	ctx := context.Background()
	oldLeaseClient := getLeaseClient
	defer func() { getLeaseClient = oldLeaseClient }()
	getLeaseClient = func(leaseNameSpace string) (v1.LeaseInterface, error) {
		return nil, errors.New("lease-client-error")
	}
	err := CreateKubernetesLease(ctx, testNameSpace, "testLeaseName")
	assert.Error(t, err)
	assert.Equal(t, "lease-client-error", err.Error())
}

func TestDeleteKubernetesLease_Success(t *testing.T) {
	testNameSpace := "TestNameSpace"
	ctx := context.Background()
	oldLeaseClient := getLeaseClient
	defer func() { getLeaseClient = oldLeaseClient }()
	fakeCS := fake.NewClientset()
	getLeaseClient = func(leaseNameSpace string) (v1.LeaseInterface, error) {
		leaseClient := fakeCS.CoordinationV1().Leases(leaseNameSpace)
		return leaseClient, nil
	}
	err := CreateKubernetesLease(ctx, testNameSpace, "testLeaseName")
	assert.Nil(t, err)
	err = DeleteKubernetesLease(ctx, testNameSpace, "testLeaseName")
	assert.Nil(t, err)
}

func TestDeleteKubernetesLease_ClientError(t *testing.T) {
	testNameSpace := "TestNameSpace"
	ctx := context.Background()
	oldLeaseClient := getLeaseClient
	defer func() { getLeaseClient = oldLeaseClient }()
	getLeaseClient = func(leaseNameSpace string) (v1.LeaseInterface, error) {
		return nil, errors.New("lease-client-error")
	}
	err := DeleteKubernetesLease(ctx, testNameSpace, "testLeaseName")
	assert.Error(t, err)
	assert.Equal(t, "lease-client-error", err.Error())
}

// Below test case will test when no lease available to delete
// k8's API will send 404 error code stating leaseName doesn't exist
func TestDeleteKubernetesLease_Error(t *testing.T) {
	testNameSpace := "TestNameSpace"
	ctx := context.Background()
	oldLeaseClient := getLeaseClient
	defer func() { getLeaseClient = oldLeaseClient }()
	fakeCS := fake.NewClientset()
	getLeaseClient = func(leaseNameSpace string) (v1.LeaseInterface, error) {
		leaseClient := fakeCS.CoordinationV1().Leases(leaseNameSpace)
		return leaseClient, nil
	}
	err := DeleteKubernetesLease(ctx, testNameSpace, "testLeaseName")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
