// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package process

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A live_output process streams more than MaxLogBytes, complete and in order,
// and writes no spool file.
func TestLiveOutputHasNoCapAndNoSpool(t *testing.T) {
	cfg := DefaultConfig(t.TempDir())
	cfg.MaxLogBytes = 1024
	client := setupTestServerWithConfig(t, cfg)

	const n = 30 * 1024 * 1024
	proc := start(t, client, &ateenvv1alpha.StartProcessRequest{
		Command:    []string{"sh", "-c", "head -c 31457280 /dev/zero | tr '\\0' x; echo -n err >&2"},
		LiveOutput: true,
	})
	stdout, stderr, exit := collect(t, client, &ateenvv1alpha.StreamProcessOutputRequest{ProcessId: proc.GetProcessId(), Follow: true})
	if len(stdout) != n || strings.Trim(stdout, "x") != "" {
		t.Fatalf("stdout: got %d bytes (want %d of 'x')", len(stdout), n)
	}
	if stderr != "err" || exit.GetExitCode() != 0 {
		t.Fatalf("stderr %q exit %d, want \"err\" 0", stderr, exit.GetExitCode())
	}
	if spools, _ := filepath.Glob(filepath.Join(cfg.LogDir, "*")); len(spools) != 0 {
		t.Fatalf("live_output wrote spool files: %v", spools)
	}
}

// A client can resume at an offset inside the kept window (after a broken
// stream), and an offset already dropped fails with OUT_OF_RANGE.
func TestLiveOutputResumeAndDropped(t *testing.T) {
	client := setupTestServer(t)
	// 16 MiB of output: more than the 4 MiB kept after streaming.
	proc := start(t, client, &ateenvv1alpha.StartProcessRequest{
		Command:    []string{"sh", "-c", "head -c 16777216 /dev/zero | tr '\\0' y"},
		LiveOutput: true,
	})
	stdout, _, _ := collect(t, client, &ateenvv1alpha.StreamProcessOutputRequest{ProcessId: proc.GetProcessId(), Follow: true})
	if len(stdout) != 16<<20 {
		t.Fatalf("stdout: got %d bytes, want %d", len(stdout), 16<<20)
	}
	resumed, _, _ := collect(t, client, &ateenvv1alpha.StreamProcessOutputRequest{ProcessId: proc.GetProcessId(), Follow: true, StdoutOffset: 16<<20 - 1000})
	if len(resumed) != 1000 {
		t.Fatalf("resume near the end: got %d bytes, want 1000", len(resumed))
	}
	stream, err := client.StreamProcessOutput(context.Background(), &ateenvv1alpha.StreamProcessOutputRequest{ProcessId: proc.GetProcessId(), Follow: true})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("offset 0 after 16 MiB: got %v, want OUT_OF_RANGE", err)
	}
}

// A process whose output nobody reads blocks on write; a signal still ends it
// and it is reaped (the blocked writer must not hold the reaper).
func TestLiveOutputBlockedWriterIsReapedAfterKill(t *testing.T) {
	client := setupTestServer(t)
	proc := start(t, client, &ateenvv1alpha.StartProcessRequest{
		Command:    []string{"sh", "-c", "yes"},
		LiveOutput: true,
	})
	time.Sleep(300 * time.Millisecond) // fill the window
	if _, err := client.SignalProcess(context.Background(), &ateenvv1alpha.SignalProcessRequest{ProcessId: proc.GetProcessId(), Signal: ateenvv1alpha.Signal_SIGNAL_KILL}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		p, err := client.GetProcess(ctx, &ateenvv1alpha.GetProcessRequest{ProcessId: proc.GetProcessId()})
		if err != nil {
			t.Fatal(err)
		}
		if p.GetState() == ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED {
			return
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatal("killed live_output process was never reaped")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
