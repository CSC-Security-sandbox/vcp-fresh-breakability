package utils

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	v1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/utils/ptr"
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

func TestGetKubernetesLease_Success(t *testing.T) {
	testNameSpace := "TestNameSpace"
	testLeaseName := "testLeaseName"
	ctx := context.Background()
	oldLeaseClient := getLeaseClient
	defer func() { getLeaseClient = oldLeaseClient }()
	fakeCS := fake.NewClientset()
	getLeaseClient = func(leaseNameSpace string) (v1.LeaseInterface, error) {
		leaseClient := fakeCS.CoordinationV1().Leases(leaseNameSpace)
		return leaseClient, nil
	}

	// First create a lease
	err := CreateKubernetesLease(ctx, testNameSpace, testLeaseName)
	assert.Nil(t, err)

	// Then get the lease
	lease, err := GetKubernetesLease(ctx, testNameSpace, testLeaseName)
	assert.Nil(t, err)
	assert.NotNil(t, lease)
	assert.Equal(t, testLeaseName, lease.Name)
	assert.Equal(t, testNameSpace, lease.Namespace)
}

func TestGetKubernetesLease_ClientError(t *testing.T) {
	testNameSpace := "TestNameSpace"
	testLeaseName := "testLeaseName"
	ctx := context.Background()
	oldLeaseClient := getLeaseClient
	defer func() { getLeaseClient = oldLeaseClient }()
	getLeaseClient = func(leaseNameSpace string) (v1.LeaseInterface, error) {
		return nil, errors.New("lease-client-error")
	}

	lease, err := GetKubernetesLease(ctx, testNameSpace, testLeaseName)
	assert.Error(t, err)
	assert.Nil(t, lease)
	assert.Equal(t, "lease-client-error", err.Error())
}

func TestGetKubernetesLease_NotFound(t *testing.T) {
	testNameSpace := "TestNameSpace"
	testLeaseName := "nonExistentLease"
	ctx := context.Background()
	oldLeaseClient := getLeaseClient
	defer func() { getLeaseClient = oldLeaseClient }()
	fakeCS := fake.NewClientset()
	getLeaseClient = func(leaseNameSpace string) (v1.LeaseInterface, error) {
		leaseClient := fakeCS.CoordinationV1().Leases(leaseNameSpace)
		return leaseClient, nil
	}

	// Try to get a lease that doesn't exist
	lease, err := GetKubernetesLease(ctx, testNameSpace, testLeaseName)
	assert.Error(t, err)
	assert.Nil(t, lease)
	assert.Contains(t, err.Error(), "not found")
}

func TestLeaseExists_True(t *testing.T) {
	testNameSpace := "TestNameSpace"
	testLeaseName := "testLeaseName"
	ctx := context.Background()
	oldLeaseClient := getLeaseClient
	defer func() { getLeaseClient = oldLeaseClient }()
	fakeCS := fake.NewClientset()
	getLeaseClient = func(leaseNameSpace string) (v1.LeaseInterface, error) {
		leaseClient := fakeCS.CoordinationV1().Leases(leaseNameSpace)
		return leaseClient, nil
	}

	// First create a lease
	err := CreateKubernetesLease(ctx, testNameSpace, testLeaseName)
	assert.Nil(t, err)

	// Then check if it exists
	exists, err := LeaseExists(ctx, testNameSpace, testLeaseName)
	assert.Nil(t, err)
	assert.True(t, exists)
}

func TestLeaseExists_False(t *testing.T) {
	testNameSpace := "TestNameSpace"
	testLeaseName := "nonExistentLease"
	ctx := context.Background()
	oldLeaseClient := getLeaseClient
	defer func() { getLeaseClient = oldLeaseClient }()
	fakeCS := fake.NewClientset()
	getLeaseClient = func(leaseNameSpace string) (v1.LeaseInterface, error) {
		leaseClient := fakeCS.CoordinationV1().Leases(leaseNameSpace)
		return leaseClient, nil
	}

	// Check if a non-existent lease exists
	exists, err := LeaseExists(ctx, testNameSpace, testLeaseName)
	assert.Nil(t, err)
	assert.False(t, exists)
}

func TestLeaseExists_ClientError(t *testing.T) {
	testNameSpace := "TestNameSpace"
	testLeaseName := "testLeaseName"
	ctx := context.Background()
	oldLeaseClient := getLeaseClient
	defer func() { getLeaseClient = oldLeaseClient }()
	getLeaseClient = func(leaseNameSpace string) (v1.LeaseInterface, error) {
		return nil, errors.New("lease-client-error")
	}

	// Check lease existence when client creation fails
	exists, err := LeaseExists(ctx, testNameSpace, testLeaseName)
	assert.Error(t, err)
	assert.False(t, exists)
	assert.Equal(t, "lease-client-error", err.Error())
}

func TestContainsNotFound(t *testing.T) {
	// Test the helper function
	testCases := []struct {
		input    string
		expected bool
	}{
		{"not found", true},
		{"leases.coordination.k8s.io \"testLease\" not found", true},
		{"some error: not found", true},
		{"leases.coordination.k8s.io", true},
		{"leases.coordination.k8", true},
		{"network error", false},
		{"", false},
		{"connection timeout", false},
		{"forbidden", false},
	}

	for _, tc := range testCases {
		result := containsNotFound(tc.input)
		assert.Equal(t, tc.expected, result, "Failed for input: %s", tc.input)
	}
}

func TestGetPodIPForKubernetesLeaseHolder_Success(t *testing.T) {
	ctx := context.Background()
	oldClientset := getKubernetesClientset
	t.Cleanup(func() { getKubernetesClientset = oldClientset })

	holder := "harvest-farm-abc"
	fakeCS := fake.NewSimpleClientset(
		&coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: "harvest-lease-1", Namespace: "vcp"},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity: ptr.To(holder),
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: holder, Namespace: "vcp"},
			Status:     corev1.PodStatus{PodIP: "10.1.2.3"},
		},
	)
	getKubernetesClientset = func() (kubernetes.Interface, error) {
		return fakeCS, nil
	}

	ip, err := GetPodIPForKubernetesLeaseHolder(ctx, "vcp", "harvest-lease-1", "vcp")
	require.NoError(t, err)
	assert.Equal(t, "10.1.2.3", ip)
}

func TestGetPodIPForKubernetesLeaseHolder_EmptyHolder(t *testing.T) {
	ctx := context.Background()
	oldClientset := getKubernetesClientset
	t.Cleanup(func() { getKubernetesClientset = oldClientset })

	fakeCS := fake.NewSimpleClientset(
		&coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: "harvest-lease-1", Namespace: "vcp"},
			Spec:         coordinationv1.LeaseSpec{},
		},
	)
	getKubernetesClientset = func() (kubernetes.Interface, error) {
		return fakeCS, nil
	}

	_, err := GetPodIPForKubernetesLeaseHolder(ctx, "vcp", "harvest-lease-1", "vcp")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty holder identity")
}

func TestGetPodIPForKubernetesLeaseHolder_PodNotFound(t *testing.T) {
	ctx := context.Background()
	oldClientset := getKubernetesClientset
	t.Cleanup(func() { getKubernetesClientset = oldClientset })

	fakeCS := fake.NewSimpleClientset(
		&coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: "harvest-lease-1", Namespace: "vcp"},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity: ptr.To("missing-pod"),
			},
		},
	)
	getKubernetesClientset = func() (kubernetes.Interface, error) {
		return fakeCS, nil
	}

	_, err := GetPodIPForKubernetesLeaseHolder(ctx, "vcp", "harvest-lease-1", "vcp")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get pod")
}

func TestGetPodIPForKubernetesLeaseHolder_ClientsetError(t *testing.T) {
	ctx := context.Background()
	oldClientset := getKubernetesClientset
	t.Cleanup(func() { getKubernetesClientset = oldClientset })
	getKubernetesClientset = func() (kubernetes.Interface, error) {
		return nil, errors.New("no in-cluster config")
	}
	_, err := GetPodIPForKubernetesLeaseHolder(ctx, "vcp", "lease", "vcp")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no in-cluster config")
}

func TestGetPodIPForKubernetesLeaseHolder_PodIPNotReady(t *testing.T) {
	ctx := context.Background()
	oldClientset := getKubernetesClientset
	t.Cleanup(func() { getKubernetesClientset = oldClientset })

	holder := "harvest-farm-noip"
	fakeCS := fake.NewSimpleClientset(
		&coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: "harvest-lease-noip", Namespace: "vcp"},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity: ptr.To(holder),
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: holder, Namespace: "vcp"},
			Status:     corev1.PodStatus{PodIP: ""},
		},
	)
	getKubernetesClientset = func() (kubernetes.Interface, error) {
		return fakeCS, nil
	}

	_, err := GetPodIPForKubernetesLeaseHolder(ctx, "vcp", "harvest-lease-noip", "vcp")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no pod IP")
}
