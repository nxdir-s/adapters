package adapters

import (
	"os"

	"github.com/pelletier/go-toml/v2"
)

type ErrReadCfg struct {
	name string
	err  error
}

func (e *ErrReadCfg) Error() string {
	return "failed to read " + e.name + ": " + e.err.Error()
}

type ErrUnmarshalToml struct {
	name string
	err  error
}

func (e *ErrUnmarshalToml) Error() string {
	return "failed to unmarshal " + e.name + ": " + e.err.Error()
}

type TomlAdapter[T any] struct {
	Cfg T
}

// NewTomlAdapter creates a toml adapter
func NewTomlAdapter[T any]() *TomlAdapter[T] {
	return &TomlAdapter[T]{}
}

// LoadConfig attempts to read a toml file in the current directory and returns a config
func (a *TomlAdapter[T]) LoadConfig(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return &ErrReadCfg{filename, err}
	}

	if err := toml.Unmarshal(data, &a.Cfg); err != nil {
		return &ErrUnmarshalToml{filename, err}
	}

	return nil
}

// Config returns the loaded config
func (a *TomlAdapter[T]) Config() T {
	return a.Cfg
}
