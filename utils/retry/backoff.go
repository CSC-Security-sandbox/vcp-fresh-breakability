package retry

import "time"

// ExponentialBackoffWithJitter computes a capped exponential backoff duration
// with cryptographically secure random jitter.
//
//	delay = min(base * 2^attempt, maxBackoff) + rand[0, jitterMaxMs) ms
//
// This is suitable for any retry loop that needs exponential back-off
// (e.g. IAM policy conflict retries, rate-limit retries, etc.).
func ExponentialBackoffWithJitter(attempt int, base, maxBackoff time.Duration, jitterMaxMs int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	backoff := base
	if backoff > maxBackoff {
		backoff = maxBackoff
	} else {
		for i := 0; i < attempt; i++ {
			if backoff > maxBackoff/2 {
				backoff = maxBackoff
				break
			}
			backoff *= 2
		}
	}
	if jitterMaxMs > 0 {
		jitter, err := SecureIntn(jitterMaxMs)
		if err != nil {
			jitter = 0
		}
		backoff += time.Duration(jitter) * time.Millisecond
	}
	return backoff
}
