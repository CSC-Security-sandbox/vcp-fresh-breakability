// Package utils provides a Kubernetes Lease-based distributed lock.
// One Lease per lock name in a namespace; holder identity and renew/acquisition
// times determine ownership and expiry.

package utils

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

const (
	defaultRetryWaitDuration    = 1 * time.Second
	defaultMaxLockAcquiringWait = 1 * time.Second
	defaultLeaseTTLSeconds      = 30
)

// generateTimeNow is used for lease AcquireTime/RenewTime and expiry checks.
// Tests can override it to control time-based behaviour.
var generateTimeNow = time.Now

// Lease lock TTL in seconds when not set via option. Can be overridden by env VCP_LEASE_LOCK_TTL_SEC.
func defaultLeaseDurationSeconds() int32 {
	if s := os.Getenv("VCP_LEASE_LOCK_TTL_SEC"); s != "" {
		if n, err := strconv.ParseInt(s, 10, 32); err == nil && n > 0 {
			return int32(n)
		}
	}
	return defaultLeaseTTLSeconds
}

// Locker holds Kubernetes lease-based lock state.
type Locker struct {
	leaseClient             v1.LeaseInterface
	namespace               string
	name                    string
	clientID                string
	retryWait               time.Duration
	ttl                     *time.Duration // nil = use defaultLeaseDurationSeconds()
	maxLockAcquiringWait    time.Duration
	acquired                bool
	defaultLeaseDurationSec int32
}

// Option configures a Locker.
type Option func(*Locker) error

// Namespace sets the Kubernetes namespace for the lease.
func Namespace(ns string) Option {
	return func(l *Locker) error {
		l.namespace = ns
		return nil
	}
}

// InClusterConfig uses in-cluster config to build the Kubernetes client.
func InClusterConfig() Option {
	return func(l *Locker) error {
		config, err := rest.InClusterConfig()
		if err != nil {
			return fmt.Errorf("in-cluster config: %w", err)
		}
		cs, err := kubernetes.NewForConfig(config)
		if err != nil {
			return fmt.Errorf("kubernetes client: %w", err)
		}
		l.leaseClient = cs.CoordinationV1().Leases(l.namespace)
		return nil
	}
}

// Clientset sets the Kubernetes client (for tests). LeaseClient is derived from it for the Locker's namespace.
func Clientset(c kubernetes.Interface) Option {
	return func(l *Locker) error {
		if c == nil {
			return errors.New("clientset is nil")
		}
		l.leaseClient = c.CoordinationV1().Leases(l.namespace)
		return nil
	}
}

// ClientID sets the holder identity. If empty, NewLocker uses a new UUID.
func ClientID(id string) Option {
	return func(l *Locker) error {
		l.clientID = id
		return nil
	}
}

// TTL sets the lease duration (LeaseDurationSeconds) when acquiring the lock.
func TTL(ttl time.Duration) Option {
	return func(l *Locker) error {
		l.ttl = &ttl
		return nil
	}
}

// RetryWaitDuration sets the wait between lock acquisition retries. Used by the
// caller (e.g. common activity) when retrying Lock on ErrAlreadyHeld.
func RetryWaitDuration(d time.Duration) Option {
	return func(l *Locker) error {
		l.retryWait = d
		return nil
	}
}

// MaxLockAcquiringWait sets the maximum time to spend trying to acquire the lock.
// Used by the caller (e.g. common activity) when retrying Lock on ErrAlreadyHeld.
func MaxLockAcquiringWait(d time.Duration) Option {
	return func(l *Locker) error {
		l.maxLockAcquiringWait = d
		return nil
	}
}

// NewLocker builds a Locker for the given lease name. At least one of InClusterConfig or Clientset must be applied.
// If the lease does not exist, it is created with LeaseTransitions 0. ctx is used for the initial Get/Create.
func NewLocker(ctx context.Context, name string, options ...Option) (*Locker, error) {
	l := &Locker{
		name:                    name,
		namespace:               "default",
		retryWait:               defaultRetryWaitDuration,
		maxLockAcquiringWait:    defaultMaxLockAcquiringWait,
		defaultLeaseDurationSec: defaultLeaseDurationSeconds(),
	}
	if l.clientID == "" {
		l.clientID = uuid.New().String()
	}
	for _, opt := range options {
		if err := opt(l); err != nil {
			return nil, err
		}
	}
	if l.leaseClient == nil {
		return nil, errors.New("lease client not set: use InClusterConfig() or Clientset()")
	}
	_, err := l.leaseClient.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("get lease: %w", err)
		}
		// Create lease with LeaseTransitions 0
		newLease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: l.namespace},
			Spec:       coordinationv1.LeaseSpec{LeaseTransitions: ptr.To[int32](0)},
		}
		if _, createErr := l.leaseClient.Create(ctx, newLease, metav1.CreateOptions{}); createErr != nil {
			return nil, fmt.Errorf("create lease: %w", createErr)
		}
		return l, nil
	}
	return l, nil
}

// ErrAlreadyHeld is returned by Lock when the lease is currently held by another
// client (or a concurrent update caused a conflict). Callers (e.g. a common
// activity) should retry Lock on this error with backoff.
var ErrAlreadyHeld = errors.New("lock already held")

// Lock performs a single attempt to acquire the lease. If the lease is unheld or
// expired (RenewTime + LeaseDurationSeconds in the past), it sets HolderIdentity,
// AcquireTime, RenewTime, LeaseTransitions, and optional LeaseDurationSeconds and
// updates the lease. Returns ErrAlreadyHeld if the lease is held and not expired,
// or if Update returns conflict; the caller is responsible for retrying with backoff.
// Logger may be nil.
func (l *Locker) Lock(ctx context.Context, logger log.Logger) error {
	lease, err := l.getOrCreateLease(ctx)
	if err != nil {
		return err
	}

	// If lease is held, check if renew time has expired
	if lease.Spec.HolderIdentity != nil {
		if logger != nil {
			logger.Debug(fmt.Sprintf("HolderIdentity for client : %s : %s", l.clientID, *lease.Spec.HolderIdentity))
		}
		leaseDuration := time.Duration(l.defaultLeaseDurationSec) * time.Second
		if lease.Spec.LeaseDurationSeconds != nil {
			leaseDuration = time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second
		}
		if lease.Spec.RenewTime != nil && lease.Spec.RenewTime.Add(leaseDuration).After(generateTimeNow()) {
			if logger != nil {
				logger.Debug(fmt.Sprintf("Lock renew time is valid for lock : %s", l.name))
			}
			return ErrAlreadyHeld
		}
	}

	// Nobody holds the lock or lease expired: acquire
	transitions := int32(0)
	if lease.Spec.LeaseTransitions != nil {
		transitions = *lease.Spec.LeaseTransitions
	}
	transitions++

	now := generateTimeNow()
	lease.Spec.HolderIdentity = ptr.To(l.clientID)
	lease.Spec.AcquireTime = &metav1.MicroTime{Time: now}
	lease.Spec.RenewTime = &metav1.MicroTime{Time: now}
	lease.Spec.LeaseTransitions = &transitions
	// Only set LeaseDurationSeconds when TTL option was set
	if l.ttl != nil && l.ttl.Seconds() > 0 {
		sec := int32(l.ttl.Seconds())
		if sec < 1 {
			sec = 1
		}
		lease.Spec.LeaseDurationSeconds = &sec
	}

	_, err = l.leaseClient.Update(ctx, lease, metav1.UpdateOptions{})
	if err != nil {
		if logger != nil {
			logger.Warn(fmt.Sprintf("Got error while updating lease : %s, %s", l.name, err.Error()))
		}
		if apierrors.IsConflict(err) {
			return ErrAlreadyHeld
		}
		return fmt.Errorf("update lease: %w", err)
	}
	l.acquired = true
	return nil
}

// getOrCreateLease returns the lease, creating it with LeaseTransitions 0 if missing.
func (l *Locker) getOrCreateLease(ctx context.Context) (*coordinationv1.Lease, error) {
	lease, err := l.leaseClient.Get(ctx, l.name, metav1.GetOptions{})
	if err == nil {
		return lease, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get lease: %w", err)
	}
	// Lease missing: create minimal lease then get
	newLease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: l.name, Namespace: l.namespace},
		Spec:       coordinationv1.LeaseSpec{LeaseTransitions: ptr.To[int32](0)},
	}
	if _, createErr := l.leaseClient.Create(ctx, newLease, metav1.CreateOptions{}); createErr != nil {
		if !apierrors.IsAlreadyExists(createErr) {
			return nil, fmt.Errorf("create lease: %w", createErr)
		}
	}
	lease, err = l.leaseClient.Get(ctx, l.name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get lease after create: %w", err)
	}
	return lease, nil
}

// RenewLock verifies the current holder is this client, then sets RenewTime to now and updates the lease.
// Mirrors reference RenewLock using _verifyLock.
func (l *Locker) RenewLock(ctx context.Context) error {
	lease, err := l.verifyLock(ctx)
	if err != nil {
		return fmt.Errorf("renew lock: %w", err)
	}
	lease.Spec.RenewTime = &metav1.MicroTime{Time: generateTimeNow()}
	_, err = l.leaseClient.Update(ctx, lease, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("renew lock: update lease: %w", err)
	}
	return nil
}

// ClientID returns the holder identity for this Locker (used when releasing the lock from another activity).
func (l *Locker) ClientID() string { return l.clientID }

// Unlock verifies the current holder is this client, then clears HolderIdentity, AcquireTime, RenewTime,
// and LeaseDurationSeconds and updates the lease. Mirrors reference _verifyLock + Unlock.
func (l *Locker) Unlock(ctx context.Context) error {
	lease, err := l.verifyLock(ctx)
	if err != nil {
		return fmt.Errorf("unlock: %w", err)
	}
	lease.Spec.HolderIdentity = nil
	lease.Spec.AcquireTime = nil
	lease.Spec.RenewTime = nil
	lease.Spec.LeaseDurationSeconds = nil
	_, err = l.leaseClient.Update(ctx, lease, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("unlock: update lease: %w", err)
	}
	l.acquired = false
	return nil
}

// verifyLock gets the lease and verifies this client is the holder (mirrors reference _verifyLock).
func (l *Locker) verifyLock(ctx context.Context) (*coordinationv1.Lease, error) {
	lease, err := l.leaseClient.Get(ctx, l.name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, errors.New("lease not present")
		}
		return nil, fmt.Errorf("get lease: %w", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		return nil, errors.New("no lease holder for locker")
	}
	if *lease.Spec.HolderIdentity != l.clientID {
		return nil, fmt.Errorf("current client is not holding the lease (holder: %s)", *lease.Spec.HolderIdentity)
	}
	return lease, nil
}
