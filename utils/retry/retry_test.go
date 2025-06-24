package retry

import (
	"runtime"
	"testing"

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
