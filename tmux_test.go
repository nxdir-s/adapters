package adapters

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type ErrMockExec struct{}

func (e *ErrMockExec) Error() string {
	return "error executing command"
}

type ErrCmdArgs struct{}

func (e *ErrCmdArgs) Error() string {
	return "command arguments dont match"
}

type mockCmdOpt func(m *mockCmd)

func withCmdExec(returnArgs ...interface{}) mockCmdOpt {
	return func(m *mockCmd) {
		m.On("Exec", mock.Anything, mock.Anything).Return(returnArgs...)
	}
}

type mockCmd struct {
	mock.Mock
}

func newMockCmd(opts ...mockCmdOpt) *mockCmd {
	cmd := &mockCmd{}

	for _, opt := range opts {
		opt(cmd)
	}

	return cmd
}

func (m *mockCmd) Exec(ctx context.Context, cmd *exec.Cmd) (io.Reader, error) {
	args := m.Called(ctx, cmd)
	return args.Get(0).(io.Reader), args.Error(1)
}

const (
	TestTmuxSession string = "TmuxUnitTests"
	TestTmuxWindow  string = "TmuxUnitTests"
	TestTmuxOutput  string = "test output"
	TestTmuxCmd     string = "test-cmd"
)

func TestHasSession(t *testing.T) {
	var cases = []struct {
		session     string
		expectedVal int
		expectedErr error
		opts        []TmuxOpt
	}{
		{
			session:     TestTmuxSession,
			expectedVal: TmuxSessionExists,
			expectedErr: nil,
			opts: []TmuxOpt{
				WithCmdAdapter(newMockCmd(
					withCmdExec(bytes.NewReader(
						[]byte(TestTmuxOutput),
					), nil),
				)),
			},
		},
		{
			session:     TestTmuxSession,
			expectedVal: TmuxSessionNotExists,
			expectedErr: &ErrHasSession{&ErrTest{}},
			opts: []TmuxOpt{
				WithCmdAdapter(newMockCmd(
					withCmdExec(bytes.NewReader([]byte{}), &ErrTest{}),
				)),
			},
		},
		{
			session:     TestTmuxSession,
			expectedVal: TmuxSessionNotExists,
			expectedErr: &ErrNilCmd{},
			opts:        []TmuxOpt{},
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			adapter := NewTmuxAdapter(ctx, tt.opts...)

			out, err := adapter.HasSession(ctx, tt.session)

			assert.Equal(t, tt.expectedErr, err)
			if err == nil {
				assert.Equal(t, tt.expectedVal, out)
			}
		})
	}
}

func TestNewSession(t *testing.T) {
	var cases = []struct {
		session     string
		expectedErr error
		opts        []TmuxOpt
	}{
		{
			session:     TestTmuxSession,
			expectedErr: nil,
			opts: []TmuxOpt{
				WithCmdAdapter(newMockCmd(
					withCmdExec(bytes.NewReader(
						[]byte(TestTmuxOutput),
					), nil),
				)),
			},
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			adapter := NewTmuxAdapter(ctx, tt.opts...)

			err := adapter.NewSession(ctx, tt.session)

			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func TestTmuxErrors(t *testing.T) {
	var err error

	err = &ErrHasSession{&ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrHasSession"})
	}

	err = &ErrNewSession{TestTmuxSession, &ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrNewSession"})
	}

	err = &ErrAttachSession{TestTmuxSession, &ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrAttachSession"})
	}

	err = &ErrNewWindow{TestTmuxWindow, &ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrNewWindow"})
	}

	err = &ErrSelectWindow{TestTmuxWindow, &ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrSelectWindow"})
	}

	err = &ErrSendKeys{TestTmuxCmd, &ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrSendKeys"})
	}

	err = &ErrNilCmd{}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrNilCmd"})
	}
}
