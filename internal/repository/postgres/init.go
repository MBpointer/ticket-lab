package postgres

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresRepo реализует контракт PostgreSQL-репозитория.
type PostgresRepo struct {
	pool *pgxpool.Pool
}

// MustInit создает пул соединений с PostgreSQL,
// проверяет физическое соединение с базой данных.
func MustInit(ctx context.Context) *PostgresRepo {
	dsn := os.Getenv("POSTGRES_DSN")

	if dsn == "" {
		dsn = "postgres://postgres:postgres@127.0.0.1:5439/ticket_db?sslmode=disable"
	}

	// Отдельный таймаут защищает запуск приложения
	// от бесконечного ожидания PostgreSQL.
	healthCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(healthCtx, dsn)
	if err != nil {
		slog.Error(
			"❌ [PostgreSQL] Pool creation failed",
			"error",
			err,
		)
		os.Exit(1)
	}

	// Физический Ping подтверждает, что база действительно доступна.
	if err := pool.Ping(healthCtx); err != nil {
		slog.Error(
			"❌ CRITICAL DATABASE ERROR: Application cannot start!",
			"error",
			err,
		)

		pool.Close()
		os.Exit(1)
	}

	slog.Info(
		"🏛️ [PostgreSQL] Physical connection successfully verified (Ping OK).",
	)

	return &PostgresRepo{pool: pool}
}

// Close безопасным образом закрывает пул соединений.
func (p *PostgresRepo) Close() {
	if p.pool != nil {
		slog.Info("🔌 [PostgreSQL] Closing connection pool...")
		p.pool.Close()
	}
}
