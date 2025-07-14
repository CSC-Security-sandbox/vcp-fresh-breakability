package utils

import (
	"context"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/utils/ptr"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
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

// creates k8's lease for harvest farm
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
