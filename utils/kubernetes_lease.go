package utils

import (
	"context"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

var (
	getLeaseClient = func(nameSpace string) (v1.LeaseInterface, error) {
		config, err := rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
		clientSet, err := kubernetes.NewForConfig(config)
		if err != nil {
			return nil, err
		}
		return clientSet.CoordinationV1().Leases(nameSpace), nil
	}
)

// deletes k8's lease if lease has no pollers so that harvest farm can be scaled down
func DeleteKubernetesLease(ctx context.Context, leaseNameSpace, leaseName string) error {
	leaseClient, err := getLeaseClient(leaseNameSpace)
	if err != nil {
		return err
	}
	err = leaseClient.Delete(ctx, leaseName, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil
}

// GetKubernetesLease gets a Kubernetes lease from a given namespace
func GetKubernetesLease(ctx context.Context, leaseNameSpace, leaseName string) (*coordinationv1.Lease, error) {
	leaseClient, err := getLeaseClient(leaseNameSpace)
	if err != nil {
		return nil, err
	}
	lease, err := leaseClient.Get(ctx, leaseName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return lease, nil
}

// LeaseExists checks if a Kubernetes lease exists in a given namespace.
func LeaseExists(ctx context.Context, leaseNameSpace, leaseName string) (bool, error) {
	leaseClient, err := getLeaseClient(leaseNameSpace)
	if err != nil {
		return false, err
	}
	_, err = leaseClient.Get(ctx, leaseName, metav1.GetOptions{})
	if err != nil {
		// If the error is "not found", the lease doesn't exist
		if containsNotFound(err.Error()) {
			return false, nil
		}
		// Other errors (like network issues) should be returned
		return false, err
	}
	return true, nil
}

// helper function to check if error message contains "not found"
func containsNotFound(errMsg string) bool {
	if len(errMsg) == 0 {
		return false
	}

	// Check for exact "not found" match
	if errMsg == "not found" {
		return true
	}

	// Check if message ends with "not found"
	if len(errMsg) >= 9 && errMsg[len(errMsg)-9:] == "not found" {
		return true
	}

	// Check if message contains Kubernetes-style not found error
	if len(errMsg) > 0 && (errMsg[:min(len(errMsg), 22)] == "leases.coordination.k8" ||
		errMsg[:min(len(errMsg), 28)] == "leases.coordination.k8s.io") {
		return true
	}

	return false
}

// CreateKubernetesLease creates a Kubernetes lease for harvest farm
func CreateKubernetesLease(ctx context.Context, leaseNameSpace, leaseName string) error {
	leaseClient, err := getLeaseClient(leaseNameSpace)
	if err != nil {
		return err
	}
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: leaseNameSpace,
		},
		Spec: coordinationv1.LeaseSpec{
			LeaseTransitions: ptr.To[int32](0),
		},
	}
	_, err = leaseClient.Create(ctx, lease, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	return nil
}
