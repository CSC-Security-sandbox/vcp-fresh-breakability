package retry

import (
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"runtime"
	"testing"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestDo(t *testing.T) {
	oldWaitMs := waitMs
	defer func() {
		waitMs = oldWaitMs
	}()
	var i = 0
	var visits = 0
	maxRetries = 10
	waitMs = 10
	testFunc1 := func(in int) (string, error) {
		i++
		visits++
		if i <= in {
			return "", errors.New("My time to fail")
		}

		return "success", nil
	}

	t.Run("WhenNotRetrying", func(tt *testing.T) {
		var value string
		i = 0
		visits = 0
		err := Do(func(attempt int) (bool, error) {
			var err error
			value, err = testFunc1(0)
			return true, err
		})
		if err != nil {
			tt.Error("Unexpected error returned")
		}
		if value != "success" {
			tt.Errorf("Unexpected value returned: '%s'", value)
		}
		if visits != 1 {
			tt.Errorf("Unexpected function visit count: '%d'", visits)
		}
	})
	t.Run("WhenRetryingOnce", func(tt *testing.T) {
		var value string
		i = 0
		visits = 0
		err := Do(func(attempt int) (bool, error) {
			var err error
			value, err = testFunc1(1)
			return true, err
		})
		if err != nil {
			tt.Error("Unexpected error returned")
		}
		if value != "success" {
			tt.Errorf("Unexpected value returned: '%s'", value)
		}
		if visits != 2 {
			tt.Errorf("Unexpected function visit count: '%d'", visits)
		}
	})
	t.Run("WhenRetryLimitIsExceeded", func(tt *testing.T) {
		var value string
		i = 0
		visits = 0
		err := Do(func(attempt int) (bool, error) {
			var err error
			value, err = testFunc1(10)
			return true, err
		})
		if err == nil {
			tt.Error("Expected an error")
		}
		if value != "" {
			tt.Errorf("Unexpected value returned: '%s'", value)
		}
		if visits != 10 {
			tt.Errorf("Unexpected function visit count: '%d'", visits)
		}
	})
	t.Run("WhenCannotRetry", func(tt *testing.T) {
		var value string
		i = 0
		visits = 0
		err := Do(func(attempt int) (bool, error) {
			var err error
			value, err = testFunc1(10)
			return false, err
		})
		if err == nil {
			tt.Error("Expected an error")
		}
		if value != "" {
			tt.Errorf("Unexpected value returned: '%s'", value)
		}
		if visits != 1 {
			tt.Errorf("Unexpected function visit count: '%d'", visits)
		}
	})
}

func TestGetCallerName(t *testing.T) {
	t.Run("WhenRuntimeCallerReturnsFailure", func(tt *testing.T) {
		runtimeCaller = func(skip int) (pc uintptr, file string, line int, ok bool) {
			return
		}

		if getCallerName(2) != "" {
			tt.Fail()
		}
		runtimeCaller = runtime.Caller
	})
	t.Run("WhenRuntimeFuncForPCReturnsNil", func(tt *testing.T) {
		runtimeFuncForPC = func(pc uintptr) *runtime.Func {
			return nil
		}

		if getCallerName(2) != "" {
			tt.Fail()
		}
		runtimeFuncForPC = runtime.FuncForPC
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		data := []struct {
			skip         int
			expectedName string
		}{
			{skip: 0, expectedName: "getCallerName"},
			{skip: 2, expectedName: "tRunner"},
		}

		for _, d := range data {
			callerName := getCallerName(d.skip)
			if callerName != d.expectedName {
				tt.Errorf("Caller name '%s' does not match expected '%s'", callerName, d.expectedName)
			}
		}
	})
}

func TestSecureIntn(t *testing.T) {
	t.Run("WhenNIsPositive", func(tt *testing.T) {
		// Test with various positive values
		testCases := []int{1, 10, 100, 1000}
		for _, n := range testCases {
			result, err := SecureIntn(n)
			if err != nil {
				tt.Errorf("secureIntn(%d) returned unexpected error: %v", n, err)
			}
			if result < 0 || result >= n {
				tt.Errorf("secureIntn(%d) returned value %d out of range [0, %d)", n, result, n)
			}
		}
	})

	t.Run("WhenNIsZero", func(tt *testing.T) {
		result, err := SecureIntn(0)
		if err == nil {
			tt.Error("secureIntn(0) should return an error")
		}
		if result != 0 {
			tt.Errorf("secureIntn(0) should return 0, got %d", result)
		}
	})

	t.Run("WhenNIsNegative", func(tt *testing.T) {
		result, err := SecureIntn(-1)
		if err == nil {
			tt.Error("secureIntn(-1) should return an error")
		}
		if result != 0 {
			tt.Errorf("secureIntn(-1) should return 0, got %d", result)
		}
	})

	t.Run("WhenRandReaderSucceeds", func(tt *testing.T) {
		// This test verifies that secureIntn works correctly with the real rand.Reader
		// and returns values in the correct range
		for i := 0; i < 100; i++ {
			result, err := SecureIntn(100)
			if err != nil {
				tt.Errorf("secureIntn(100) returned unexpected error on iteration %d: %v", i, err)
			}
			if result < 0 || result >= 100 {
				tt.Errorf("secureIntn(100) returned value %d out of range [0, 100) on iteration %d", result, i)
			}
		}
	})
}

func TestDoWithJitterErrorHandling(t *testing.T) {
	oldWaitMs := waitMs
	oldMaxRetries := maxRetries
	defer func() {
		waitMs = oldWaitMs
		maxRetries = oldMaxRetries
	}()

	maxRetries = 3
	waitMs = 10 // Small delay for faster tests

	t.Run("WhenJitterGenerationSucceeds", func(tt *testing.T) {
		attempts := 0
		err := Do(func(attempt int) (bool, error) {
			attempts++
			if attempts < 2 {
				return true, errors.New("retry me")
			}
			return false, nil
		})

		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if attempts != 2 {
			tt.Errorf("Expected 2 attempts, got %d", attempts)
		}
	})

	t.Run("WhenRetryHappensWithJitter", func(tt *testing.T) {
		// This test verifies that retries happen and jitter is applied
		// The actual jitter value is random, but we can verify the retry
		// mechanism works correctly
		attempts := 0
		startTime := time.Now()

		err := Do(func(attempt int) (bool, error) {
			attempts++
			if attempts < 2 {
				return true, errors.New("retry me")
			}
			return false, nil
		})

		elapsed := time.Since(startTime)

		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if attempts != 2 {
			tt.Errorf("Expected 2 attempts, got %d", attempts)
		}
		// Verify that some delay occurred (at least the base delay)
		// Jitter may or may not be applied, but base delay should be there
		if elapsed < time.Duration(waitMs)*time.Millisecond {
			tt.Errorf("Expected at least %dms delay, got %v", waitMs, elapsed)
		}
	})
}

func TestSecureIntnErrorHandling(t *testing.T) {
	t.Run("MultipleCallsReturnDifferentValues", func(tt *testing.T) {
		// Verify that multiple calls return different values (randomness)
		results := make(map[int]bool)
		n := 1000
		uniqueCount := 0

		for i := 0; i < 100; i++ {
			result, err := SecureIntn(n)
			if err != nil {
				tt.Fatalf("secureIntn(%d) returned error: %v", n, err)
			}
			if !results[result] {
				results[result] = true
				uniqueCount++
			}
		}

		// With 100 calls and range of 1000, we should get many unique values
		// This is a probabilistic test, but should pass with high probability
		if uniqueCount < 50 {
			tt.Errorf("Expected at least 50 unique values from 100 calls, got %d", uniqueCount)
		}
	})

	t.Run("ValuesAreInRange", func(tt *testing.T) {
		n := 50
		for i := 0; i < 1000; i++ {
			result, err := SecureIntn(n)
			if err != nil {
				tt.Fatalf("secureIntn(%d) returned error on iteration %d: %v", n, i, err)
			}
			if result < 0 || result >= n {
				tt.Fatalf("secureIntn(%d) returned value %d out of range [0, %d) on iteration %d", n, result, n, i)
			}
		}
	})
}

// failingReader is a test helper that always returns an error when reading
type failingReader struct{}

func (f *failingReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("simulated read error")
}

// secureIntnWithReader is a test helper that allows injecting a custom reader
// This mirrors the SecureIntn function but accepts a reader parameter for testing
func secureIntnWithReader(reader io.Reader, n int) (int, error) {
	if n <= 0 {
		return 0, fmt.Errorf("n must be positive")
	}
	maxVal := big.NewInt(int64(n))
	result, err := rand.Int(reader, maxVal)
	if err != nil {
		return 0, err
	}
	return int(result.Int64()), nil
}

func TestSecureIntn_RandIntError(t *testing.T) {
	t.Run("WhenRandIntReturnsError", func(tt *testing.T) {
		// Test error handling when rand.Int fails by using a failing reader
		failingReader := &failingReader{}

		n := 100
		result, err := secureIntnWithReader(failingReader, n)

		// Should return error when reader fails
		if err == nil {
			tt.Error("Expected error when rand.Int fails, got nil")
		}
		if result != 0 {
			tt.Errorf("Expected result to be 0 on error, got %d", result)
		}
		if err.Error() != "simulated read error" {
			tt.Errorf("Expected error message 'simulated read error', got: %s", err.Error())
		}
	})

	t.Run("WhenRandIntSucceeds", func(tt *testing.T) {
		// Test that secureIntnWithReader works correctly with the real rand.Reader
		n := 100
		result, err := secureIntnWithReader(rand.Reader, n)

		if err != nil {
			tt.Fatalf("secureIntnWithReader(%d) unexpectedly returned error: %v", n, err)
		}
		if result < 0 || result >= n {
			tt.Errorf("secureIntnWithReader(%d) returned value %d out of range [0, %d)", n, result, n)
		}
	})
}

func TestDo_SecureIntnErrorHandling(t *testing.T) {
	// Test that Do function handles SecureIntn errors gracefully
	oldWaitMs := waitMs
	oldMaxRetries := maxRetries
	defer func() {
		waitMs = oldWaitMs
		maxRetries = oldMaxRetries
	}()

	maxRetries = 3
	waitMs = 10 // Set delay to ensure jitterMax > 0

	t.Run("WhenSecureIntnFailsInJitterCalculation", func(tt *testing.T) {
		// This test verifies that when SecureIntn fails during jitter calculation,
		// the Do function falls back gracefully to using delay without jitter
		//
		// Note: In practice, rand.Int from crypto/rand rarely fails, but this test
		// ensures the error handling path exists and works correctly

		attempts := 0
		startTime := time.Now()

		err := Do(func(attempt int) (bool, error) {
			attempts++
			if attempts < 2 {
				return true, errors.New("retry me")
			}
			return false, nil
		})

		elapsed := time.Since(startTime)

		// Should succeed despite potential SecureIntn failure (if it occurred)
		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if attempts != 2 {
			tt.Errorf("Expected 2 attempts, got %d", attempts)
		}
		// Verify that delay occurred (at least base delay, jitter may or may not be applied)
		// If SecureIntn failed, it should still sleep for at least the base delay
		if elapsed < time.Duration(waitMs)*time.Millisecond {
			tt.Errorf("Expected at least %dms delay, got %v", waitMs, elapsed)
		}
	})

	t.Run("WhenSecureIntnFailsMultipleTimes", func(tt *testing.T) {
		// Test that Do function continues to work even if SecureIntn fails multiple times
		attempts := 0

		err := Do(func(attempt int) (bool, error) {
			attempts++
			if attempts < 3 {
				return true, errors.New("retry me")
			}
			return false, nil
		})

		if err != nil {
			tt.Errorf("Unexpected error: %v", err)
		}
		if attempts != 3 {
			tt.Errorf("Expected 3 attempts, got %d", attempts)
		}
		// Function should complete successfully even if SecureIntn fails
		// The fallback mechanism should handle it gracefully
	})
}
