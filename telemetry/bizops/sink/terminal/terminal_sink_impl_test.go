package terminal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
)

type mockReader struct {
}

func (e *mockReader) Read(p []byte) (int, error) {
	return 0, errors.New("simulated read error")
}

func Test_NewTerminalBizOpsSink(t *testing.T) {
	gcpSink := NewTerminalBizOpsSink()
	if gcpSink == nil {
		t.Error("Expected non-nil sink")
	}
	if _, ok := gcpSink.(*TerminalBizOpsSink); !ok {
		t.Error("Expected type *TerminalBizOpsSink")
	}
}

func Test_TerminalInject(t *testing.T) {
	terminalSink := NewTerminalBizOpsSink()
	testParams := &entity.BizopsSinkParams{
		Date:     time.Now(),
		Timezone: "UTC",
	}
	ctx := context.Background()
	t.Run("Success", func(t *testing.T) {
		originalStdout := os.Stdout
		r, w, _ := os.Pipe()
		defer func() {
			os.Stdout = originalStdout
			_ = w.Close()
		}()

		os.Stdout = w
		input := "test from terminal sink"
		testParams.Reader = bytes.NewReader([]byte(input))
		err := terminalSink.Ingest(ctx, testParams)
		_ = w.Close() // Ensure all data is flushed before reading
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		if buf.String() != input {
			t.Errorf("Expected output %q, got %q", input, buf.String())
		}
	})
	t.Run("Invalid Params", func(t *testing.T) {
		err := terminalSink.Ingest(ctx, nil)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "sink params cannot be nil")
	})
	t.Run("Invalid Reader", func(t *testing.T) {
		testParams.Reader = nil
		err := terminalSink.Ingest(ctx, testParams)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "reader cannot be nil")
	})
	t.Run("WhenCopyFails", func(t *testing.T) {
		originalStdout := os.Stdout
		_, w, _ := os.Pipe()
		defer func() {
			os.Stdout = originalStdout
			_ = w.Close()
		}()

		os.Stdout = w
		testParams.Reader = &mockReader{}
		err := terminalSink.Ingest(ctx, testParams)
		_ = w.Close() // Ensure all data is flushed before reading
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "simulated read error")
	})
}
