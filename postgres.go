package adapters

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type ErrConnect struct {
	err error
}

func (e *ErrConnect) Error() string {
	return "error creating connection pool: " + e.err.Error()
}

type ErrQueryRow struct {
	err error
}

func (e *ErrQueryRow) Error() string {
	return "error querying row: " + e.err.Error()
}

type ErrExecQuery struct {
	err error
}

func (e *ErrExecQuery) Error() string {
	return "error executing query: " + e.err.Error()
}

type ErrNotFound struct {
	name string
}

func (e *ErrNotFound) Error() string {
	return "error no rows affected"
}

type ErrCollectRows struct {
	err error
}

func (e *ErrCollectRows) Error() string {
	return "failed to collect rows to struct: " + e.err.Error()
}

type ErrNilPgxTx struct{}

func (e *ErrNilPgxTx) Error() string {
	return "error nil PgxTx"
}

type ErrNilPgxPool struct{}

func (e *ErrNilPgxPool) Error() string {
	return "error nil PgxPool"
}

type PgxPool interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
}

// NewPgxPool creates a pgxpool.Pool
func NewPgxPool(ctx context.Context, connUrl string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, connUrl)
	if err != nil {
		return nil, &ErrConnect{err}
	}

	return pool, nil
}

type PgxTx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type PostgresOpt[T any] func(a *PostgresAdapter[T])

// WithPgxTx sets the transaction adapter
func WithPgxTx[T any](tx PgxTx) PostgresOpt[T] {
	return func(a *PostgresAdapter[T]) {
		a.tx = tx
	}
}

type PostgresRow struct {
	Collection string
	Query      string
	NamedArgs  pgx.NamedArgs
}

type PostgresRows[T any] struct {
	PostgresRow
	Data []T
}

type PostgresAdapter[T any] struct {
	conn   PgxPool
	tx     PgxTx
	logger *slog.Logger
	tracer trace.Tracer
}

// NewPostgresAdapter creates a new PostgresAdapter
func NewPostgresAdapter[T any](pool PgxPool, logger *slog.Logger, tracer trace.Tracer, opts ...PostgresOpt[T]) *PostgresAdapter[T] {
	adapter := &PostgresAdapter[T]{
		conn:   pool,
		logger: logger,
		tracer: tracer,
	}

	for _, opt := range opts {
		opt(adapter)
	}

	return adapter
}

// NewTransactionAdapter creates an adapter for executing transactions
func (a *PostgresAdapter[T]) NewTransactionAdapter(ctx context.Context) (*PostgresAdapter[T], error) {
	if a.conn == nil {
		return nil, &ErrNilPgxPool{}
	}

	tx, err := a.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}

	txAdapter := NewPostgresAdapter[T](tx, a.logger, a.tracer, WithPgxTx[T](tx))

	return txAdapter, nil
}

// Commit commits the transaction after first checking if the context has been canceled
func (a *PostgresAdapter[T]) Commit(ctx context.Context) error {
	if a.tx == nil {
		return &ErrNilPgxTx{}
	}

	select {
	case <-ctx.Done():
		ctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()

		return a.tx.Rollback(ctx)
	default:
		return a.tx.Commit(ctx)
	}
}

// Rollback initiates a transaction rollback
func (a *PostgresAdapter[T]) Rollback(ctx context.Context) error {
	if a.tx == nil {
		return &ErrNilPgxTx{}
	}

	return a.tx.Rollback(ctx)
}

// ConnectionPool returns the underlying pgxpool
func (a *PostgresAdapter[T]) ConnectionPool() PgxPool {
	return a.conn
}

// Insert creates a new row returns the id
func (a *PostgresAdapter[T]) Insert(ctx context.Context, row *PostgresRow) (int, error) {
	if a.conn == nil {
		return 0, &ErrNilPgxPool{}
	}

	ctx, span := a.tracer.Start(ctx, "INSERT "+row.Collection,
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "postgresql"),
			attribute.String("db.operation.name", "INSERT"),
			attribute.String("db.collection.name", row.Collection),
			attribute.String("db.query.summary", "INSERT "+row.Collection),
			attribute.String("db.query.text", row.Query),
		),
	)
	defer span.End()

	var id int
	if err := a.conn.QueryRow(ctx, row.Query, row.NamedArgs).Scan(&id); err != nil {
		return 0, &ErrExecQuery{err}
	}

	return id, nil
}

const InsertQuery string = `
    INSERT INTO table (
        data
    ) VALUES (
        @data
    ) RETURNING id
`

// Delete deletes the supplied row
func (a *PostgresAdapter[T]) Delete(ctx context.Context, row *PostgresRow) error {
	if a.conn == nil {
		return &ErrNilPgxPool{}
	}

	ctx, span := a.tracer.Start(ctx, "DELETE "+row.Collection,
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "postgresql"),
			attribute.String("db.operation.name", "DELETE"),
			attribute.String("db.collection.name", row.Collection),
			attribute.String("db.query.summary", "DELETE "+row.Collection),
			attribute.String("db.query.text", row.Query),
		),
	)
	defer span.End()

	resp, err := a.conn.Exec(ctx, row.Query, row.NamedArgs)
	if err != nil {
		return &ErrExecQuery{err}
	}

	if resp.RowsAffected() == 0 {
		return &ErrNotFound{}
	}

	return nil
}

const DeleteQuery string = `
    DELETE FROM table
    WHERE id = @id
`

func (a *PostgresAdapter[T]) Select(ctx context.Context, rows *PostgresRows[T]) error {
	if a.conn == nil {
		return &ErrNilPgxPool{}
	}

	ctx, span := a.tracer.Start(ctx, "SELECT "+rows.Collection,
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "postgresql"),
			attribute.String("db.operation.name", "SELECT"),
			attribute.String("db.collection.name", rows.Collection),
			attribute.String("db.query.summary", "SELECT "+rows.Collection),
			attribute.String("db.query.text", rows.Query),
		),
	)
	defer span.End()

	output, err := a.conn.Query(ctx, rows.Query, rows.NamedArgs)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return &ErrQueryRow{err}
	}

	collection, err := pgx.CollectRows(output, pgx.RowToStructByName[T])
	if err != nil {
		return err
	}

	rows.Data = collection

	return nil
}

const SelectQuery string = `
    SELECT *
    FROM table
    WHERE id = @id
`

func (a *PostgresAdapter[T]) RowExists(ctx context.Context, row *PostgresRow) (bool, error) {
	if a.conn == nil {
		return false, &ErrNilPgxPool{}
	}

	ctx, span := a.tracer.Start(ctx, "SELECT "+row.Collection,
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "postgresql"),
			attribute.String("db.operation.name", "SELECT"),
			attribute.String("db.collection.name", row.Collection),
			attribute.String("db.query.summary", "SELECT "+row.Collection+" by id"),
			attribute.String("db.query.text", row.Query),
		),
	)
	defer span.End()

	var id int
	err := a.conn.QueryRow(ctx, row.Query, row.NamedArgs).Scan(&id)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, &ErrQueryRow{err}
	}

	if id == 0 || errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}

	return true, nil
}

const RowExistsQuery string = `
    SELECT id
    FROM table
    WHERE id = @id
`
