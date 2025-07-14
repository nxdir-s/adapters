package adapters

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"
	"unicode"

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
	return "error nil connection pool"
}

type ErrInvalidStmt struct{}

func (e *ErrInvalidStmt) Error() string {
	return "invalid sql statement"
}

const (
	SelectKeyword string = "select"
	InsertKeyword string = "insert"
	DeleteKeyword string = "delete"
)

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

type PostgresOpt func(a *PostgresAdapter)

// WithPgxTx sets the transaction adapter
func WithPgxTx(tx PgxTx) PostgresOpt {
	return func(a *PostgresAdapter) {
		a.tx = tx
	}
}

func WithPostgresSpanAttrs(attrs ...attribute.KeyValue) PostgresOpt {
	return func(a *PostgresAdapter) {
		a.attributes = append(a.attributes, attrs...)
	}
}

type PostgresAdapter struct {
	conn       PgxPool
	tx         PgxTx
	logger     *slog.Logger
	tracer     trace.Tracer
	attributes []attribute.KeyValue
}

type SPostgresAdapter[T any] struct {
	*PostgresAdapter
}

// NewPostgresAdapter creates a new PostgresAdapter
func NewPostgresAdapter(pool PgxPool, logger *slog.Logger, tracer trace.Tracer, opts ...PostgresOpt) *PostgresAdapter {
	adapter := &PostgresAdapter{
		conn:   pool,
		logger: logger,
		tracer: tracer,
	}

	for _, opt := range opts {
		opt(adapter)
	}

	return adapter
}

// NewSPostgresAdapter wraps an existing PostgresAdapter and provides select into struct method
func NewSPostgresAdapter[T any](adapter *PostgresAdapter) *SPostgresAdapter[T] {
	return &SPostgresAdapter[T]{adapter}
}

// NewTransactionAdapter creates an adapter for executing transactions
func (a *PostgresAdapter) NewTransactionAdapter(ctx context.Context) (*PostgresAdapter, error) {
	if a.conn == nil {
		a.logger.Error("nil connection in PostgresAdapter")
		return nil, &ErrNilPgxPool{}
	}

	tx, err := a.conn.Begin(ctx)
	if err != nil {
		return nil, err
	}

	txAdapter := NewPostgresAdapter(tx, a.logger, a.tracer, WithPgxTx(tx))

	return txAdapter, nil
}

// Commit commits the transaction after first checking if the context has been canceled
func (a *PostgresAdapter) Commit(ctx context.Context) error {
	if a.tx == nil {
		a.logger.Error("nil PgxTx in PostgresAdapter")
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
func (a *PostgresAdapter) Rollback(ctx context.Context) error {
	if a.tx == nil {
		a.logger.Error("nil PgxTx in PostgresAdapter")
		return &ErrNilPgxTx{}
	}

	return a.tx.Rollback(ctx)
}

// ConnectionPool returns the underlying pgxpool
func (a *PostgresAdapter) ConnectionPool() PgxPool {
	return a.conn
}

// Exec executes the supplied sql statement and returns the number of rows affected
func (a *PostgresAdapter) Exec(ctx context.Context, sql string, args map[string]interface{}) (int64, error) {
	if a.conn == nil {
		a.logger.Error("nil connection in PostgresAdapter")
		return 0, &ErrNilPgxPool{}
	}

	ctx, span := a.tracer.Start(ctx, "EXEC",
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "postgresql"),
			attribute.String("db.query.text", sql),
		),
	)
	defer span.End()

	if a.attributes != nil {
		span.SetAttributes(a.attributes...)
	}

	resp, err := a.conn.Exec(ctx, sql, args)
	if err != nil {
		return 0, &ErrExecQuery{err}
	}

	return resp.RowsAffected(), nil
}

// Insert creates a new row returns the id
func (a *PostgresAdapter) Insert(ctx context.Context, sql string, args map[string]interface{}) (int, error) {
	if a.conn == nil {
		a.logger.Error("nil connection in PostgresAdapter")
		return 0, &ErrNilPgxPool{}
	}

	if !validStatement(InsertKeyword, sql) {
		return 0, &ErrInvalidStmt{}
	}

	ctx, span := a.tracer.Start(ctx, "INSERT",
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "postgresql"),
			attribute.String("db.operation.name", "INSERT"),
			attribute.String("db.query.text", sql),
		),
	)
	defer span.End()

	if a.attributes != nil {
		span.SetAttributes(a.attributes...)
	}

	var id int
	if err := a.conn.QueryRow(ctx, sql, args).Scan(&id); err != nil {
		return 0, &ErrExecQuery{err}
	}

	return id, nil
}

const ExInsertQuery string = `
    INSERT INTO table (
        data
    ) VALUES (
        @data
    ) RETURNING id
`

// Delete deletes the supplied row
func (a *PostgresAdapter) Delete(ctx context.Context, sql string, args map[string]interface{}) error {
	if a.conn == nil {
		a.logger.Error("nil connection in PostgresAdapter")
		return &ErrNilPgxPool{}
	}

	if !validStatement(DeleteKeyword, sql) {
		return &ErrInvalidStmt{}
	}

	ctx, span := a.tracer.Start(ctx, "DELETE",
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "postgresql"),
			attribute.String("db.operation.name", "DELETE"),
			attribute.String("db.query.text", sql),
		),
	)
	defer span.End()

	if a.attributes != nil {
		span.SetAttributes(a.attributes...)
	}

	resp, err := a.conn.Exec(ctx, sql, args)
	if err != nil {
		return &ErrExecQuery{err}
	}

	if resp.RowsAffected() == 0 {
		return &ErrNotFound{}
	}

	return nil
}

const ExDeleteQuery string = `
    DELETE FROM table
    WHERE id = @id
`

func (a *PostgresAdapter) RowExists(ctx context.Context, sql string, args map[string]interface{}) (bool, error) {
	if a.conn == nil {
		a.logger.Error("nil connection in PostgresAdapter")
		return false, &ErrNilPgxPool{}
	}

	ctx, span := a.tracer.Start(ctx, "SELECT",
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "postgresql"),
			attribute.String("db.operation.name", "SELECT"),
			attribute.String("db.query.text", sql),
		),
	)
	defer span.End()

	if a.attributes != nil {
		span.SetAttributes(a.attributes...)
	}

	var id int
	err := a.conn.QueryRow(ctx, sql, args).Scan(&id)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, &ErrQueryRow{err}
	}

	if id == 0 || errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}

	return true, nil
}

const ExRowExistsQuery string = `
    SELECT id
    FROM table
    WHERE id = @id
`

func (a *SPostgresAdapter[T]) Select(ctx context.Context, sql string, args map[string]interface{}) ([]T, error) {
	if a.conn == nil {
		a.logger.Error("nil connection in SPostgresAdapter")
		return nil, &ErrNilPgxPool{}
	}

	if !validStatement(SelectKeyword, sql) {
		return nil, &ErrInvalidStmt{}
	}

	ctx, span := a.tracer.Start(ctx, "SELECT",
		trace.WithLinks(trace.LinkFromContext(ctx)),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system.name", "postgresql"),
			attribute.String("db.operation.name", "SELECT"),
			attribute.String("db.query.text", sql),
		),
	)
	defer span.End()

	if a.attributes != nil {
		span.SetAttributes(a.attributes...)
	}

	output, err := a.conn.Query(ctx, sql, args)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, &ErrQueryRow{err}
	}

	var rows []T
	if rows, err = pgx.CollectRows(output, pgx.RowToStructByName[T]); err != nil {
		return nil, err
	}

	return rows, nil
}

const ExSelectQuery string = `
    SELECT *
    FROM table
    WHERE id = @id
`

func validStatement(keyword string, sql string) bool {
	stmt := strings.TrimLeftFunc(sql, unicode.IsSpace)

	return strings.ToLower(keyword) == strings.ToLower(stmt[:len(keyword)])
}
