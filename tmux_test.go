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
	TmuxSessionName string = "TmuxUnitTests"
	TmuxWindowName  string = "TmuxUnitTests"

	TmuxSessionExists    string = "HasSession should return 0 if a tmux session exists"
	TmuxSessionNotExists string = "HasSession should return 1 if a tmux session does not exists"

	TmuxNewSession string = "NewSession should create a new tmux session"
)

func TestHasSession(t *testing.T) {
	var cases = []struct {
		name      string
		expected  int
		shouldErr bool
	}{
		{
			name:      TmuxSessionExists,
			expected:  SessionExists,
			shouldErr: false,
		},
		{
			name:      TmuxSessionNotExists,
			expected:  SessionNotExists,
			shouldErr: true,
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	cmd := exec.CommandContext(ctx, Alias, HasSessionCmd, "-t", TmuxSessionName)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			adapter := NewTmuxAdapter(NewCmdMock(cmd, tc.shouldErr))

			if out := adapter.HasSession(ctx, TmuxSessionName); out != tc.expected {
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
			name:      TmuxNewSession,
			expected:  nil,
			shouldErr: false,
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	cmd := exec.CommandContext(ctx, Alias, NewSessionCmd, "-d", "-s", TmuxSessionName, "-n", TmuxWindowName)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			adapter := NewTmuxAdapter(NewCmdMock(cmd, tc.shouldErr))

			if out := adapter.NewSession(ctx, TmuxWindowName); out != tc.expected {
				t.Errorf("got %d, want %d", out, tc.expected)
			}
		})
	}
}
