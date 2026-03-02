package common

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
)

func RunShellScriptCtx(ctx context.Context, scriptPath string, args ...string) error {
	info, err := os.Stat(scriptPath)
	if err != nil {
		return fmt.Errorf("script stat failed: %w", err)
	}
	if info.IsDir() {
		return errors.New("script path is a directory")
	}

	cmd := exec.CommandContext(ctx, "/bin/bash", append([]string{scriptPath}, args...)...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	if err := cmd.Run(); err != nil {
		// Distinguish between normal non‑zero exit, context cancellation, and other errors.
		if ee, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("script exited with code %d: %w", ee.ExitCode(), err)
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return fmt.Errorf("script interrupted (%v): %w", ctx.Err(), err)
		}
		return fmt.Errorf("script execution failed: %w", err)
	}
	return nil
}
