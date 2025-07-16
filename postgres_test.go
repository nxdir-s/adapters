package adapters

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

type mockPoolOpt func(m *mockPool)

func withPgxExec(returnArgs ...interface{}) mockPoolOpt {
	return func(m *mockPool) {
		m.On("Exec", mock.Anything, mock.Anything, mock.Anything).Return(returnArgs...)
	}
}

func withBegin(returnArgs ...interface{}) mockPoolOpt {
	return func(m *mockPool) {
		m.On("Begin", mock.Anything).Return(returnArgs...)
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

type mockPgxRowOpt func(m *mockPgxRow)

func withPgxScan(fn mockScanFunc) mockPgxRowOpt {
	return func(m *mockPgxRow) {
		m.scan = fn
	}
}

type mockScanFunc func(dest ...interface{}) error

type mockPgxRow struct {
	scan mockScanFunc
}

func newMockPgxRow(fn mockScanFunc) *mockPgxRow {
	return &mockPgxRow{
		scan: fn,
	}
}

func (m *mockPgxRow) Scan(dest ...interface{}) error {
	if m.scan != nil {
		return m.scan(dest...)
	}

	return nil
}

const (
	TestPgxArg       string = "testdata"
	TestRowID        int    = 1
	TestRowsAffected int64  = 10
)

func TestPgExec(t *testing.T) {
	cases := []struct {
		sql         string
		args        map[string]interface{}
		expectedOut int64
		expectedErr error
		opts        []PostgresOpt
	}{
		{
			sql: ExInsertQuery,
			args: map[string]interface{}{
				"data": TestPgxArg,
			},
			expectedOut: 0,
			expectedErr: nil,
			opts: []PostgresOpt{
				WithPgxPool(newMockPool(
					withPgxExec(pgconn.CommandTag{}, nil),
				)),
				WithPgSpanAttrs(
					attribute.String(TestAttrKey, TestAttrVal),
				),
			},
		},
		{
			sql: ExInsertQuery,
			args: map[string]interface{}{
				"data": TestPgxArg,
			},
			expectedOut: 0,
			expectedErr: &ErrExecQuery{&ErrTest{}},
			opts: []PostgresOpt{
				WithPgxPool(newMockPool(
					withPgxExec(pgconn.CommandTag{}, &ErrTest{}),
				)),
			},
		},
		{
			sql:         ExInsertQuery,
			args:        map[string]interface{}{},
			expectedOut: 0,
			expectedErr: &ErrNilPgxPool{},
			opts:        []PostgresOpt{},
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			adapter := NewPostgresAdapter(ctx, logger, otel.Tracer("postgres"), tt.opts...)

			out, err := adapter.Exec(ctx, tt.sql, tt.args)

			assert.Equal(t, tt.expectedOut, out)
			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func TestPgInsert(t *testing.T) {
	cases := []struct {
		sql         string
		args        map[string]interface{}
		expectedOut int
		expectedErr error
		opts        []PostgresOpt
	}{
		{
			sql: ExInsertQuery,
			args: map[string]interface{}{
				"data": TestPgxArg,
			},
			expectedOut: TestRowID,
			expectedErr: nil,
			opts: []PostgresOpt{
				WithPgxPool(newMockPool(
					withQueryRow(newMockPgxRow(
						func(dest ...interface{}) error {
							*dest[0].(*int) = TestRowID
							return nil
						},
					)),
				)),
				WithPgSpanAttrs(
					attribute.String(TestAttrKey, TestAttrVal),
				),
			},
		},
		{
			sql: ExInsertQuery,
			args: map[string]interface{}{
				"data": TestPgxArg,
			},
			expectedOut: 0,
			expectedErr: &ErrPgInsert{&ErrTest{}},
			opts: []PostgresOpt{
				WithPgxPool(newMockPool(
					withQueryRow(newMockPgxRow(
						func(dest ...interface{}) error {
							return &ErrTest{}
						},
					)),
				)),
			},
		},
		{
			sql: ExSelectQuery,
			args: map[string]interface{}{
				"data": TestPgxArg,
			},
			expectedOut: 0,
			expectedErr: &ErrInvalidStmt{},
			opts: []PostgresOpt{
				WithPgxPool(newMockPool(
					withQueryRow(newMockPgxRow(
						func(dest ...interface{}) error {
							*dest[0].(*int) = TestRowID
							return nil
						},
					)),
				)),
			},
		},
		{
			sql: ExInsertQuery,
			args: map[string]interface{}{
				"data": TestPgxArg,
			},
			expectedOut: 0,
			expectedErr: &ErrNilPgxPool{},
			opts:        []PostgresOpt{},
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			adapter := NewPostgresAdapter(ctx, logger, otel.Tracer("postgres"), tt.opts...)

			out, err := adapter.Insert(ctx, tt.sql, tt.args)

			assert.Equal(t, tt.expectedOut, out)
			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func TestPgDelete(t *testing.T) {
	cases := []struct {
		sql         string
		args        map[string]interface{}
		expectedErr error
		opts        []PostgresOpt
	}{
		{
			sql: ExDeleteQuery,
			args: map[string]interface{}{
				"id": TestPgxArg,
			},
			expectedErr: nil,
			opts: []PostgresOpt{
				WithPgxPool(newMockPool(
					withPgxExec(pgconn.NewCommandTag("1"), nil),
				)),
				WithPgSpanAttrs(
					attribute.String(TestAttrKey, TestAttrVal),
				),
			},
		},
		{
			sql: ExDeleteQuery,
			args: map[string]interface{}{
				"id": TestPgxArg,
			},
			expectedErr: &ErrNoRows{},
			opts: []PostgresOpt{
				WithPgxPool(newMockPool(
					withPgxExec(pgconn.CommandTag{}, nil),
				)),
			},
		},
		{
			sql: ExDeleteQuery,
			args: map[string]interface{}{
				"id": TestPgxArg,
			},
			expectedErr: &ErrPgDelete{&ErrTest{}},
			opts: []PostgresOpt{
				WithPgxPool(newMockPool(
					withPgxExec(pgconn.CommandTag{}, &ErrTest{}),
				)),
			},
		},
		{
			sql: ExInsertQuery,
			args: map[string]interface{}{
				"id": TestPgxArg,
			},
			expectedErr: &ErrInvalidStmt{},
			opts: []PostgresOpt{
				WithPgxPool(newMockPool(
					withPgxExec(pgconn.CommandTag{}, &ErrTest{}),
				)),
			},
		},
		{
			sql:         ExDeleteQuery,
			args:        map[string]interface{}{},
			expectedErr: &ErrNilPgxPool{},
			opts:        []PostgresOpt{},
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			adapter := NewPostgresAdapter(ctx, logger, otel.Tracer("postgres"), tt.opts...)

			err := adapter.Delete(ctx, tt.sql, tt.args)

			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func TestPgRowExists(t *testing.T) {
	cases := []struct {
		sql         string
		args        map[string]interface{}
		expectedOut bool
		expectedErr error
		opts        []PostgresOpt
	}{
		{
			sql: ExRowExistsQuery,
			args: map[string]interface{}{
				"id": TestPgxArg,
			},
			expectedOut: true,
			expectedErr: nil,
			opts: []PostgresOpt{
				WithPgxPool(newMockPool(
					withQueryRow(newMockPgxRow(
						func(dest ...interface{}) error {
							*dest[0].(*int) = TestRowID
							return nil
						},
					)),
				)),
				WithPgSpanAttrs(
					attribute.String(TestAttrKey, TestAttrVal),
				),
			},
		},
		{
			sql: ExRowExistsQuery,
			args: map[string]interface{}{
				"id": TestPgxArg,
			},
			expectedOut: false,
			expectedErr: &ErrRowExists{&ErrTest{}},
			opts: []PostgresOpt{
				WithPgxPool(newMockPool(
					withQueryRow(newMockPgxRow(
						func(dest ...interface{}) error {
							return &ErrTest{}
						},
					)),
				)),
			},
		},
		{
			sql: ExRowExistsQuery,
			args: map[string]interface{}{
				"id": TestPgxArg,
			},
			expectedOut: false,
			expectedErr: nil,
			opts: []PostgresOpt{
				WithPgxPool(newMockPool(
					withQueryRow(newMockPgxRow(
						func(dest ...interface{}) error {
							return pgx.ErrNoRows
						},
					)),
				)),
			},
		},
		{
			sql: ExInsertQuery,
			args: map[string]interface{}{
				"id": TestPgxArg,
			},
			expectedOut: false,
			expectedErr: &ErrInvalidStmt{},
			opts: []PostgresOpt{
				WithPgxPool(newMockPool(
					withQueryRow(newMockPgxRow(
						func(dest ...interface{}) error {
							*dest[0].(*int) = TestRowID
							return nil
						},
					)),
				)),
			},
		},
		{
			sql: ExRowExistsQuery,
			args: map[string]interface{}{
				"id": TestPgxArg,
			},
			expectedOut: false,
			expectedErr: &ErrNilPgxPool{},
			opts:        []PostgresOpt{},
		},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			adapter := NewPostgresAdapter(ctx, logger, otel.Tracer("postgres"), tt.opts...)

			out, err := adapter.RowExists(ctx, tt.sql, tt.args)

			assert.Equal(t, tt.expectedOut, out)
			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func TestPostgresErrors(t *testing.T) {
	var err error

	err = &ErrPgxConn{&ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrPgxConn"})
	}

	err = &ErrPgInsert{&ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrPgInsert"})
	}

	err = &ErrPgDelete{&ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrPgDelete"})
	}

	err = &ErrQueryRow{&ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrQueryRow"})
	}

	err = &ErrExecQuery{&ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrExecQuery"})
	}

	err = &ErrNoRows{}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrNotFound"})
	}

	err = &ErrCollectRows{&ErrTest{}}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrCollectRows"})
	}

	err = &ErrNilPgxTx{}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrNilPgxTx"})
	}

	err = &ErrNilPgxPool{}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrNilPgxPool"})
	}

	err = &ErrInvalidStmt{}
	if len(err.Error()) == 0 {
		t.Error(&ErrMissingMsg{"ErrInvalidStmt"})
	}
}
