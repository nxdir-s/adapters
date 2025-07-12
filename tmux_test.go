package adapters

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"reflect"
	"testing"
)

type ErrMockExec struct{}

func (e *ErrMockExec) Error() string {
	return "error executing command"
}

type ErrCmdArgs struct{}

func (e *ErrCmdArgs) Error() string {
	return "command arguments dont match"
}

type CmdMock struct {
	cmd       *exec.Cmd
	shouldErr bool
}

func NewCmdMock(cmd *exec.Cmd, shouldErr bool) *CmdMock {
	return &CmdMock{
		cmd:       cmd,
		shouldErr: shouldErr,
	}
}

func (a *CmdMock) Exec(ctx context.Context, cmd *exec.Cmd) (io.Reader, error) {
	if !reflect.DeepEqual(a.cmd.Args, cmd.Args) {
		fmt.Fprintf(os.Stdout, "CmdMock: arguments are different: %+v %+v", a.cmd.Args, cmd.Args)
		return nil, &ErrCmdArgs{}
	}

	switch a.shouldErr {
	case true:
		return bytes.NewReader([]byte("")), &ErrMockExec{}
	case false:
		return bytes.NewReader([]byte("")), nil
	default:
		return bytes.NewReader([]byte("")), nil
	}
}

const (
	TestTmuxSession string = "TmuxUnitTests"
	TestTmuxWindow  string = "TmuxUnitTests"
)

func TestHasSession(t *testing.T) {
	var cases = []struct {
		name      string
		expected  int
		shouldErr bool
	}{
		{
			name:      "should return 0 if a tmux session exists",
			expected:  TmuxSessionExists,
			shouldErr: false,
		},
		{
			name:      "should return 1 if a tmux session does not exists",
			expected:  TmuxSessionNotExists,
			shouldErr: true,
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	cmd := exec.CommandContext(ctx, Alias, TmuxHasSessionCmd, "-t", TestTmuxSession)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			adapter := NewTmuxAdapter(NewCmdMock(cmd, tc.shouldErr))

			if out := adapter.HasSession(ctx, TestTmuxSession); out != tc.expected {
				t.Errorf("got %d, want %d", out, tc.expected)
			}
		})
	}
}

func TestNewSession(t *testing.T) {
	var cases = []struct {
		name      string
		expected  error
		shouldErr bool
	}{
		{
			name:      "should create a new tmux session",
			expected:  nil,
			shouldErr: false,
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	cmd := exec.CommandContext(ctx, Alias, TmuxNewSessionCmd, "-d", "-s", TestTmuxSession, "-n", TestTmuxWindow)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			adapter := NewTmuxAdapter(NewCmdMock(cmd, tc.shouldErr))

			if out := adapter.NewSession(ctx, TestTmuxWindow); out != tc.expected {
				t.Errorf("got %d, want %d", out, tc.expected)
			}
		})
	}
}
