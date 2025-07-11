package adapters

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
)

type ErrNewSession struct {
	session string
	err     error
}

func (e *ErrNewSession) Error() string {
	return "error creating new session named '" + e.session + "': " + e.err.Error()
}

type ErrAttachSession struct {
	session string
	err     error
}

func (e *ErrAttachSession) Error() string {
	return "error attaching to session '" + e.session + "': " + e.err.Error()
}

type ErrNewWindow struct {
	window string
	err    error
}

func (e *ErrNewWindow) Error() string {
	return "error creating new window named '" + e.window + "': " + e.err.Error()
}

type ErrSelectWindow struct {
	window string
	err    error
}

func (e *ErrSelectWindow) Error() string {
	return "error selecting " + e.window + " window: " + e.err.Error()
}

type ErrSendKeys struct {
	cmd string
	err error
}

func (e *ErrSendKeys) Error() string {
	return "error executing " + SendKeysCmd + " with cmd '" + e.cmd + "': " + e.err.Error()
}

const Alias string = "tmux"

const (
	SessionExists int = iota
	SessionNotExists
)

const (
	EnterCmd        string = "C-m"
	HasSessionCmd   string = "has-session"
	NewSessionCmd   string = "new-session"
	NewWindowCmd    string = "new-window"
	SelectWindowCmd string = "select-window"
	AttachCmd       string = "attach-session"
	SendKeysCmd     string = "send-keys"
)

type Command interface {
	Exec(context.Context, *exec.Cmd) (io.Reader, error)
}

type TmuxAdapter struct {
	cmd Command
}

// NewTmuxAdapter creates a tmux adapter
func NewTmuxAdapter(adapter Command) *TmuxAdapter {
	return &TmuxAdapter{
		cmd: adapter,
	}
}

// HasSession checks for an already existing tmux session
func (a *TmuxAdapter) HasSession(ctx context.Context, session string) int {
	cmd := exec.CommandContext(ctx, Alias, HasSessionCmd, "-t", session)

	fmt.Fprintf(os.Stdout, "checking for existing session '%s'\n", session)

	output, err := a.cmd.Exec(ctx, cmd)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s failed: %s\n", HasSessionCmd, err.Error())

		return SessionNotExists
	}

	if _, err := io.Copy(os.Stdout, output); err != nil {
		return SessionNotExists
	}

	return SessionExists
}

// NewSession creates a new tmux session
func (a *TmuxAdapter) NewSession(ctx context.Context, name string) error {
	cmd := exec.CommandContext(ctx, Alias, NewSessionCmd, "-d", "-s", name, "-n", name)

	fmt.Fprintf(os.Stdout, "creating new session named '%s'\n", name)

	output, err := a.cmd.Exec(ctx, cmd)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s failed: %s\n", NewSessionCmd, err.Error())

		return &ErrNewSession{name, err}
	}

	if _, err := io.Copy(os.Stdout, output); err != nil {
		return err
	}

	return nil
}

// AttachSession attempts attaching to a tmux session
func (a *TmuxAdapter) AttachSession(ctx context.Context, session string) error {
	cmd := exec.CommandContext(ctx, Alias, AttachCmd, "-t", session)
	cmd.Stdin = os.Stdin

	output, err := a.cmd.Exec(ctx, cmd)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s failed: %s\n", AttachCmd, err.Error())

		return &ErrAttachSession{session, err}
	}

	if _, err := io.Copy(os.Stdout, output); err != nil {
		return err
	}

	return nil
}

// NewWindow creates a new tmux window
func (a *TmuxAdapter) NewWindow(ctx context.Context, session string, name string) error {
	cmd := exec.CommandContext(ctx, Alias, NewWindowCmd, "-t", session, "-n", name)

	output, err := a.cmd.Exec(ctx, cmd)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s failed: %s\n", NewWindowCmd, err.Error())

		return &ErrNewWindow{name, err}
	}

	if _, err := io.Copy(os.Stdout, output); err != nil {
		return err
	}

	return nil
}

// SelectWindow selects a tmux window
func (a *TmuxAdapter) SelectWindow(ctx context.Context, session string, window string) error {
	cmd := exec.CommandContext(ctx, Alias, SelectWindowCmd, "-t", session+":"+window)

	output, err := a.cmd.Exec(ctx, cmd)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s failed: %s\n", SelectWindowCmd, err.Error())

		return &ErrSelectWindow{window, err}
	}

	if _, err := io.Copy(os.Stdout, output); err != nil {
		return err
	}

	return nil
}
