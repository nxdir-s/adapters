package adapters

import (
	"bytes"
	"context"
	"io"
	"os/exec"
)

type CmdAdapter struct{}

func NewCmdAdapter(ctx context.Context) (*CmdAdapter, error) {
	return &CmdAdapter{}, nil
}

func (a *CmdAdapter) Exec(ctx context.Context, cmd *exec.Cmd) (io.Reader, error) {
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(output), nil
}
