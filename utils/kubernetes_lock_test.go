package utils

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestNewLocker_LeaseAlreadyPresent(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	ns, name := "test-ns", "test-lease"
	_, err := fakeCS.CoordinationV1().Leases(ns).Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       coordinationv1.LeaseSpec{LeaseTransitions: ptrToInt32(0)},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	locker, err := NewLocker(ctx, name, Namespace(ns), Clientset(fakeCS))
	require.NoError(t, err)
	require.NotNil(t, locker)
	require.NotEmpty(t, locker.clientID)
	assert.Equal(t, ns, locker.namespace)
	assert.Equal(t, name, locker.name)
}

func TestNewLocker_LeaseNotPresent(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	ns, name := "test-ns", "new-lease"

	locker, err := NewLocker(ctx, name, Namespace(ns), Clientset(fakeCS))
	require.NoError(t, err)
	require.NotNil(t, locker)

	// Lease should have been created with LeaseTransitions 0
	lease, err := fakeCS.CoordinationV1().Leases(ns).Get(ctx, name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, lease.Spec.LeaseTransitions)
	assert.Equal(t, int32(0), *lease.Spec.LeaseTransitions)
}

func TestNewLocker_DefaultValues(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	locker, err := NewLocker(ctx, "x", Namespace("default"), Clientset(fakeCS))
	require.NoError(t, err)
	assert.Equal(t, defaultRetryWaitDuration, locker.retryWait)
	assert.Equal(t, defaultMaxLockAcquiringWait, locker.maxLockAcquiringWait)
	assert.NotEmpty(t, locker.clientID)
}

func TestNewLocker_RequiresClient(t *testing.T) {
	ctx := context.Background()
	_, err := NewLocker(ctx, "x", Namespace("default"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lease client not set")
}

func TestLock_LeaseHeldRenewTimeValid(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	ns, name := "lock-ns", "held-lease"
	now := metav1.NewMicroTime(time.Now())
	_, err := fakeCS.CoordinationV1().Leases(ns).Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptrTo("other-client"),
			RenewTime:            &now,
			LeaseDurationSeconds: ptrToInt32(60),
			LeaseTransitions:     ptrToInt32(1),
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	locker, err := NewLocker(ctx, name, Namespace(ns), Clientset(fakeCS),
		ClientID("me"), MaxLockAcquiringWait(100*time.Millisecond), RetryWaitDuration(10*time.Millisecond))
	require.NoError(t, err)

	err = locker.Lock(ctx, nil)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrAlreadyHeld))
}

func TestLock_LeaseHeldRenewTimePassed_Success(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	ns, name := "lock-ns", "expired-lease"
	past := metav1.NewMicroTime(time.Now().Add(-1 * time.Hour))
	_, err := fakeCS.CoordinationV1().Leases(ns).Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptrTo("old-holder"),
			RenewTime:            &past,
			LeaseDurationSeconds: ptrToInt32(30),
			LeaseTransitions:     ptrToInt32(1),
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	locker, err := NewLocker(ctx, name, Namespace(ns), Clientset(fakeCS), ClientID("new-holder"))
	require.NoError(t, err)

	err = locker.Lock(ctx, nil)
	require.NoError(t, err)
	assert.True(t, locker.acquired)

	lease, err := fakeCS.CoordinationV1().Leases(ns).Get(ctx, name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "new-holder", *lease.Spec.HolderIdentity)
}

func TestLock_LeaseHeldLeaseDurationSecondsNil_ExpiredUsingDefaultTTL(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	ns, name := "lock-ns", "nil-ttl-lease"
	// RenewTime 1 hour ago; LeaseDurationSeconds nil -> default 30s -> expired
	past := metav1.NewMicroTime(time.Now().Add(-1 * time.Hour))
	_, err := fakeCS.CoordinationV1().Leases(ns).Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:   ptrTo("old-holder"),
			RenewTime:        &past,
			LeaseTransitions: ptrToInt32(1),
			// LeaseDurationSeconds intentionally nil
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	locker, err := NewLocker(ctx, name, Namespace(ns), Clientset(fakeCS), ClientID("new-holder"))
	require.NoError(t, err)

	err = locker.Lock(ctx, nil)
	require.NoError(t, err)
	assert.True(t, locker.acquired)
	lease, _ := fakeCS.CoordinationV1().Leases(ns).Get(ctx, name, metav1.GetOptions{})
	require.NotNil(t, lease.Spec.HolderIdentity)
	assert.Equal(t, "new-holder", *lease.Spec.HolderIdentity)
}

func TestLock_SuccessWhenUnheld(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	ns, name := "lock-ns", "unheld-lease"
	locker, err := NewLocker(ctx, name, Namespace(ns), Clientset(fakeCS), ClientID("only-client"))
	require.NoError(t, err)

	err = locker.Lock(ctx, nil)
	require.NoError(t, err)
	assert.True(t, locker.acquired)

	lease, err := fakeCS.CoordinationV1().Leases(ns).Get(ctx, name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, lease.Spec.HolderIdentity)
	assert.Equal(t, "only-client", *lease.Spec.HolderIdentity)
}

func TestRenewLock_LeaseNotPresent(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	locker, err := NewLocker(ctx, "missing", Namespace("ns"), Clientset(fakeCS), ClientID("c1"))
	require.NoError(t, err)
	// Don't create the lease; delete if NewLocker created it
	_ = fakeCS.CoordinationV1().Leases("ns").Delete(ctx, "missing", metav1.DeleteOptions{})

	err = locker.RenewLock(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not present")
}

func TestRenewLock_HolderIdentityNil(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	ns, name := "ns", "rlease"
	_, err := fakeCS.CoordinationV1().Leases(ns).Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       coordinationv1.LeaseSpec{LeaseTransitions: ptrToInt32(0)},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	locker, err := NewLocker(ctx, name, Namespace(ns), Clientset(fakeCS), ClientID("c1"))
	require.NoError(t, err)

	err = locker.RenewLock(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no lease holder")
}

func TestRenewLock_HolderIdentityDifferentClient(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	ns, name := "ns", "rlease2"
	now := metav1.NewMicroTime(time.Now())
	_, err := fakeCS.CoordinationV1().Leases(ns).Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:   ptrTo("other"),
			RenewTime:        &now,
			LeaseTransitions: ptrToInt32(1),
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	locker, err := NewLocker(ctx, name, Namespace(ns), Clientset(fakeCS), ClientID("me"))
	require.NoError(t, err)

	err = locker.RenewLock(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not holding the lease")
}

func TestRenewLock_Success(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	ns, name := "ns", "rlease3"
	oldTime := metav1.NewMicroTime(time.Now().Add(-1 * time.Minute))
	_, err := fakeCS.CoordinationV1().Leases(ns).Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:   ptrTo("me"),
			RenewTime:        &oldTime,
			LeaseTransitions: ptrToInt32(1),
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	locker, err := NewLocker(ctx, name, Namespace(ns), Clientset(fakeCS), ClientID("me"))
	require.NoError(t, err)

	err = locker.RenewLock(ctx)
	require.NoError(t, err)

	lease, err := fakeCS.CoordinationV1().Leases(ns).Get(ctx, name, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, lease.Spec.RenewTime)
	assert.True(t, lease.Spec.RenewTime.Time.After(oldTime.Time))
}

func TestUnlock_LeaseNotPresent(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	locker, err := NewLocker(ctx, "gone", Namespace("ns"), Clientset(fakeCS), ClientID("c1"))
	require.NoError(t, err)
	_ = fakeCS.CoordinationV1().Leases("ns").Delete(ctx, "gone", metav1.DeleteOptions{})

	err = locker.Unlock(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lease not present")
}

func TestUnlock_HolderIdentityNil(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	ns, name := "ns", "ulease"
	_, err := fakeCS.CoordinationV1().Leases(ns).Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       coordinationv1.LeaseSpec{LeaseTransitions: ptrToInt32(0)},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	locker, err := NewLocker(ctx, name, Namespace(ns), Clientset(fakeCS), ClientID("c1"))
	require.NoError(t, err)

	err = locker.Unlock(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no lease holder")
}

func TestUnlock_HolderIdentityDifferentClient(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	ns, name := "ns", "ulease2"
	now := metav1.NewMicroTime(time.Now())
	_, err := fakeCS.CoordinationV1().Leases(ns).Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:   ptrTo("other"),
			RenewTime:        &now,
			LeaseTransitions: ptrToInt32(1),
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	locker, err := NewLocker(ctx, name, Namespace(ns), Clientset(fakeCS), ClientID("me"))
	require.NoError(t, err)

	err = locker.Unlock(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not holding the lease")
}

func TestUnlock_Success(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	ns, name := "ns", "ulease3"
	locker, err := NewLocker(ctx, name, Namespace(ns), Clientset(fakeCS), ClientID("me"))
	require.NoError(t, err)
	err = locker.Lock(ctx, nil)
	require.NoError(t, err)

	err = locker.Unlock(ctx)
	require.NoError(t, err)
	assert.False(t, locker.acquired)

	lease, err := fakeCS.CoordinationV1().Leases(ns).Get(ctx, name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Nil(t, lease.Spec.HolderIdentity)
	assert.Nil(t, lease.Spec.AcquireTime)
	assert.Nil(t, lease.Spec.RenewTime)
	assert.Nil(t, lease.Spec.LeaseDurationSeconds)
}

func ptrTo(s string) *string    { return &s }
func ptrToInt32(n int32) *int32 { return &n }

func TestDefaultLeaseDurationSeconds_FromEnv(t *testing.T) {
	orig := os.Getenv("VCP_LEASE_LOCK_TTL_SEC")
	defer func() {
		require.NoError(t, os.Setenv("VCP_LEASE_LOCK_TTL_SEC", orig))
	}()

	require.NoError(t, os.Setenv("VCP_LEASE_LOCK_TTL_SEC", "45"))
	assert.Equal(t, int32(45), defaultLeaseDurationSeconds())
}

func TestDefaultLeaseDurationSeconds_InvalidEnvFallsBack(t *testing.T) {
	orig := os.Getenv("VCP_LEASE_LOCK_TTL_SEC")
	defer func() {
		require.NoError(t, os.Setenv("VCP_LEASE_LOCK_TTL_SEC", orig))
	}()

	require.NoError(t, os.Setenv("VCP_LEASE_LOCK_TTL_SEC", "invalid"))
	assert.Equal(t, int32(defaultLeaseTTLSeconds), defaultLeaseDurationSeconds())
}

func TestClientsetOption_NilClientset(t *testing.T) {
	l := &Locker{namespace: "default"}
	err := Clientset(nil)(l)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "clientset is nil")
}

func TestInClusterConfigOption_ErrorWhenNotInCluster(t *testing.T) {
	l := &Locker{namespace: "default"}
	err := InClusterConfig()(l)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "in-cluster config")
}

func TestTTLOption_SetsValue(t *testing.T) {
	l := &Locker{}
	require.NoError(t, TTL(500*time.Millisecond)(l))
	require.NotNil(t, l.ttl)
	assert.Equal(t, 500*time.Millisecond, *l.ttl)
}

func TestNewLocker_OptionReturnsError(t *testing.T) {
	ctx := context.Background()
	badOpt := func(*Locker) error { return errors.New("bad option") }
	_, err := NewLocker(ctx, "lease", badOpt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad option")
}

func TestNewLocker_GetLeaseError(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	fakeCS.PrependReactor("get", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("get failed")
	})
	_, err := NewLocker(ctx, "lease", Namespace("ns"), Clientset(fakeCS))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get lease")
}

func TestNewLocker_CreateLeaseError(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	fakeCS.PrependReactor("create", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("create failed")
	})
	_, err := NewLocker(ctx, "lease", Namespace("ns"), Clientset(fakeCS))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create lease")
}

func TestLock_UpdateConflictReturnsAlreadyHeld(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	locker, err := NewLocker(ctx, "lease", Namespace("ns"), Clientset(fakeCS), ClientID("me"))
	require.NoError(t, err)
	fakeCS.PrependReactor("update", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "lease", errors.New("conflict"))
	})
	err = locker.Lock(ctx, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrAlreadyHeld))
}

func TestLock_UpdateGenericError(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	locker, err := NewLocker(ctx, "lease", Namespace("ns"), Clientset(fakeCS), ClientID("me"))
	require.NoError(t, err)
	fakeCS.PrependReactor("update", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("update failed")
	})
	err = locker.Lock(ctx, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "update lease")
}

func TestLock_TTLSmallerThanSecondSetsOneSecond(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	locker, err := NewLocker(ctx, "lease", Namespace("ns"), Clientset(fakeCS), ClientID("me"), TTL(500*time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, locker.Lock(ctx, nil))
	lease, err := fakeCS.CoordinationV1().Leases("ns").Get(ctx, "lease", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, lease.Spec.LeaseDurationSeconds)
	assert.Equal(t, int32(1), *lease.Spec.LeaseDurationSeconds)
}

func TestGetOrCreateLease_GetError(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	locker, err := NewLocker(ctx, "lease", Namespace("ns"), Clientset(fakeCS))
	require.NoError(t, err)
	fakeCS.PrependReactor("get", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("get failed")
	})
	_, err = locker.getOrCreateLease(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get lease")
}

func TestGetOrCreateLease_CreateAlreadyExistsThenGetFails(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	locker, err := NewLocker(ctx, "lease", Namespace("ns"), Clientset(fakeCS))
	require.NoError(t, err)
	call := 0
	fakeCS.PrependReactor("get", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		call++
		if call == 1 {
			return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "lease")
		}
		return true, nil, errors.New("get after create failed")
	})
	fakeCS.PrependReactor("create", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewAlreadyExists(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "lease")
	})
	_, err = locker.getOrCreateLease(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get lease after create")
}

func TestRenewLock_UpdateError(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	ns, name := "ns", "rlease-update-fail"
	now := metav1.NewMicroTime(time.Now())
	_, err := fakeCS.CoordinationV1().Leases(ns).Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:   ptrTo("me"),
			RenewTime:        &now,
			LeaseTransitions: ptrToInt32(1),
		},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	locker, err := NewLocker(ctx, name, Namespace(ns), Clientset(fakeCS), ClientID("me"))
	require.NoError(t, err)
	fakeCS.PrependReactor("update", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("update failed")
	})
	err = locker.RenewLock(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "renew lock: update lease")
}

func TestLockerClientID_ReturnsConfiguredID(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	locker, err := NewLocker(ctx, "lease", Namespace("ns"), Clientset(fakeCS), ClientID("my-client"))
	require.NoError(t, err)
	assert.Equal(t, "my-client", locker.ClientID())
}

func TestUnlock_UpdateError(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	ns, name := "ns", "ulease-update-fail"
	locker, err := NewLocker(ctx, name, Namespace(ns), Clientset(fakeCS), ClientID("me"))
	require.NoError(t, err)
	require.NoError(t, locker.Lock(ctx, nil))
	fakeCS.PrependReactor("update", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("update failed")
	})
	err = locker.Unlock(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unlock: update lease")
}

func TestVerifyLock_GetError(t *testing.T) {
	ctx := context.Background()
	fakeCS := fake.NewClientset()
	locker, err := NewLocker(ctx, "lease", Namespace("ns"), Clientset(fakeCS), ClientID("me"))
	require.NoError(t, err)
	fakeCS.PrependReactor("get", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("get failed")
	})
	_, err = locker.verifyLock(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get lease")
}
