package errors

import (
	"context"
	"testing"

	"github.com/go-openapi/runtime"
)

func TestParseOntapError(t *testing.T) {
	ontapErr := New("code: 2222; This is an ontap error")
	ctx := context.Background()
	t.Run("WhenErrorIsNil", func(tt *testing.T) {
		err := ParseOntapError(ctx, nil)
		if err != nil {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIsNil", func(tt *testing.T) {
		err := ParseOntapError(ctx, ontapErr)
		if err == nil {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIs202", func(tt *testing.T) {
		ontapErr200 := runtime.NewAPIError("", "", 202)
		err := ParseOntapError(ctx, ontapErr200)
		if err != nil {
			tt.Fail()
		}
	})
	t.Run("WhenErrorIs201", func(tt *testing.T) {
		ontapErr200 := runtime.NewAPIError("", "", 201)
		err := ParseOntapError(ctx, ontapErr200)
		if err != nil {
			tt.Fail()
		}
	})
}
