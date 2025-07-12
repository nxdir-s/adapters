package adapters

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/mock"
)

type mockPoolOpt func(m *mockPool)

func withExec(returnArgs ...interface{}) mockPoolOpt {
	return func(m *mockPool) {
		m.On("Exec", mock.Anything, mock.Anything, mock.Anything).Return(returnArgs...)
	}
}

func withQuery(returnArgs ...interface{}) mockPoolOpt {
	return func(m *mockPool) {
		m.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(returnArgs...)
	}
}

func withQueryRow(returnArgs ...interface{}) mockPoolOpt {
	return func(m *mockPool) {
		m.On("QueryRow", mock.Anything, mock.Anything, mock.Anything).Return(returnArgs...)
	}
}

func withBegin(returnArgs ...interface{}) mockPoolOpt {
	return func(m *mockPool) {
		m.On("Begin", mock.Anything).Return(returnArgs...)
	}
}

type mockPool struct {
	mock.Mock
}

func newMockPool(opts ...mockPoolOpt) *mockPool {
	pool := &mockPool{}

	for _, opt := range opts {
		opt(pool)
	}

	return pool
}

func (m *mockPool) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	margs := m.Called(ctx, sql, args)
	return margs.Get(0).(pgconn.CommandTag), margs.Error(1)
}

func (m *mockPool) Begin(ctx context.Context) (pgx.Tx, error) {
	args := m.Called(ctx)
	return args.Get(0).(pgx.Tx), args.Error(1)
}

func (m *mockPool) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	margs := m.Called(ctx, sql, args)
	return margs.Get(0).(pgx.Rows), margs.Error(1)
}

func (m *mockPool) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	margs := m.Called(ctx, sql, args)
	return margs.Get(0).(pgx.Row)
}
