package adapters

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

type ErrHasSession struct {
	err error
}

func (e *ErrHasSession) Error() string {
	return "error checking if session exists: " + e.err.Error()
}

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
	return "error executing " + TmuxSendKeysCmd + " with cmd '" + e.cmd + "': " + e.err.Error()
}

type ErrNilCmd struct{}

func (e *ErrNilCmd) Error() string {
	return "error nil CmdAdapter"
}

const Alias string = "tmux"

const (
	TmuxSessionExists int = iota
	TmuxSessionNotExists
)

const (
	TmuxEnterCmd        string = "C-m"
	TmuxHasSessionCmd   string = "has-session"
	TmuxNewSessionCmd   string = "new-session"
	TmuxNewWindowCmd    string = "new-window"
	TmuxSelectWindowCmd string = "select-window"
	TmuxAttachCmd       string = "attach-session"
	TmuxSendKeysCmd     string = "send-keys"
)

type Command interface {
	Exec(context.Context, *exec.Cmd) (io.Reader, error)
}

type TmuxOpt func(a *TmuxAdapter)

func WithCmdAdapter(cmd Command) TmuxOpt {
	return func(a *TmuxAdapter) {
		a.cmd = cmd
	}
}

type TmuxAdapter struct {
	cmd Command
}

// NewTmuxAdapter creates a tmux adapter
func NewTmuxAdapter(ctx context.Context, opts ...TmuxOpt) *TmuxAdapter {
	adapter := &TmuxAdapter{}

	for _, opt := range opts {
		opt(adapter)
	}

	return adapter
}

// HasSession checks for an already existing tmux session
func (a *TmuxAdapter) HasSession(ctx context.Context, session string) (int, error) {
	if a.cmd == nil {
		return TmuxSessionNotExists, &ErrNilCmd{}
	}

	cmd := exec.CommandContext(ctx, Alias, TmuxHasSessionCmd, "-t", session)

	fmt.Fprintf(os.Stdout, "checking for existing session '%s'\n", session)

	output, err := a.cmd.Exec(ctx, cmd)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s failed: %s\n", TmuxHasSessionCmd, err.Error())

		return TmuxSessionNotExists, &ErrHasSession{err}
	}

	go func() {
		if _, err := io.Copy(os.Stdout, output); err != nil {
			fmt.Fprintf(os.Stdout, "error copying '%s' output to Stdout: %s\n", TmuxHasSessionCmd, err.Error())
			return
		}
	}()

	return TmuxSessionExists, nil
}

// NewSession creates a new tmux session
func (a *TmuxAdapter) NewSession(ctx context.Context, name string) error {
	if a.cmd == nil {
		return &ErrNilCmd{}
	}

	cmd := exec.CommandContext(ctx, Alias, TmuxNewSessionCmd, "-d", "-s", name, "-n", name)

	fmt.Fprintf(os.Stdout, "creating new session named '%s'\n", name)

	output, err := a.cmd.Exec(ctx, cmd)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s failed: %s\n", TmuxNewSessionCmd, err.Error())

		return &ErrNewSession{name, err}
	}

	go func() {
		if _, err := io.Copy(os.Stdout, output); err != nil {
			fmt.Fprintf(os.Stdout, "error copying '%s' output to Stdout: %s\n", TmuxNewSessionCmd, err.Error())
			return
		}
	}()

	return nil
}

// AttachSession attempts attaching to a tmux session
func (a *TmuxAdapter) AttachSession(ctx context.Context, session string) error {
	if a.cmd == nil {
		return &ErrNilCmd{}
	}

	cmd := exec.CommandContext(ctx, Alias, TmuxAttachCmd, "-t", session)
	cmd.Stdin = os.Stdin

	output, err := a.cmd.Exec(ctx, cmd)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s failed: %s\n", TmuxAttachCmd, err.Error())

		return &ErrAttachSession{session, err}
	}

	go func() {
		if _, err := io.Copy(os.Stdout, output); err != nil {
			fmt.Fprintf(os.Stdout, "error copying '%s' output to Stdout: %s\n", TmuxAttachCmd, err.Error())
			return
		}
	}()

	return nil
}

// SendKeys executes the supplied command
func (a *TmuxAdapter) SendKeys(ctx context.Context, cmd []string, session string, window string) error {
	if a.cmd == nil {
		return &ErrNilCmd{}
	}

	cmdArgs := []string{TmuxSendKeysCmd, "-t", session + ":" + window}
	cmdArgs = append(cmdArgs, cmd...)

	command := exec.CommandContext(ctx, Alias, cmdArgs...)

	output, err := a.cmd.Exec(ctx, command)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s failed: %s\n", TmuxSendKeysCmd, err.Error())

		return &ErrSendKeys{strings.Join(cmdArgs, " "), err}
	}

	go func() {
		if _, err := io.Copy(os.Stdout, output); err != nil {
			fmt.Fprintf(os.Stdout, "error copying '%s' output to Stdout: %s\n", TmuxSendKeysCmd, err.Error())
			return
		}
	}()

	return nil
}

// NewWindow creates a new tmux window
func (a *TmuxAdapter) NewWindow(ctx context.Context, session string, name string) error {
	if a.cmd == nil {
		return &ErrNilCmd{}
	}

	cmd := exec.CommandContext(ctx, Alias, TmuxNewWindowCmd, "-t", session, "-n", name)

	output, err := a.cmd.Exec(ctx, cmd)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s failed: %s\n", TmuxNewWindowCmd, err.Error())

		return &ErrNewWindow{name, err}
	}

	go func() {
		if _, err := io.Copy(os.Stdout, output); err != nil {
			fmt.Fprintf(os.Stdout, "error copying '%s' output to Stdout: %s\n", TmuxNewWindowCmd, err.Error())
			return
		}
	}()

	return nil
}

// SelectWindow selects a tmux window
func (a *TmuxAdapter) SelectWindow(ctx context.Context, session string, window string) error {
	if a.cmd == nil {
		return &ErrNilCmd{}
	}

	cmd := exec.CommandContext(ctx, Alias, TmuxSelectWindowCmd, "-t", session+":"+window)

	output, err := a.cmd.Exec(ctx, cmd)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s failed: %s\n", TmuxSelectWindowCmd, err.Error())

		return &ErrSelectWindow{window, err}
	}

	go func() {
		if _, err := io.Copy(os.Stdout, output); err != nil {
			fmt.Fprintf(os.Stdout, "error copying '%s' output to Stdout: %s\n", TmuxSelectWindowCmd, err.Error())
			return
		}
	}()

	return nil
}
