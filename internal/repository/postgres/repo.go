package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"ticket-lab/internal/domain"
)

// SaveOrdersBatch выполняет пакетную высокопроизводительную запись заказов
// в PostgreSQL и атомарно обновляет статусы соответствующих мест на 'RESERVED'.
//
// Запросы оптимизированы под Highload: вместо поштучной отправки в цикле
// вся пачка данных улетает в базу данных за 2 массовых сетевых запроса.
// Использование ON CONFLICT (id) DO NOTHING гарантирует идемпотентность.
func (p *PostgresRepo) SaveOrdersBatch(ctx context.Context, orders []*domain.Order) error {
	if len(orders) == 0 {
		return nil
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		// Если транзакция не закоммичена, defer безопасно её откатит
		_ = tx.Rollback(ctx)
	}()

	// =========================================================================
	// 1. МАССОВАЯ ВСТАВКА ЗАКАЗОВ ЗА ОДИН СЕТЕВОЙ ЗАПРОС
	// =========================================================================
	orderQuery := `INSERT INTO orders (id, ticket_id, user_id, status, created_at) VALUES %s ON CONFLICT (id) DO NOTHING`

	orderStrings := make([]string, len(orders))
	orderArgs := make([]any, 0, len(orders)*5)

	for i, order := range orders {
		n := i * 5
		orderStrings[i] = fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)", n+1, n+2, n+3, n+4, n+5)
		orderArgs = append(orderArgs, order.ID, order.TicketID, order.UserID, order.Status, order.CreatedAt)
	}

	finalOrderQuery := fmt.Sprintf(orderQuery, strings.Join(orderStrings, ", "))
	_, err = tx.Exec(ctx, finalOrderQuery, orderArgs...)
	if err != nil {
		slog.Error("❌ [PostgreSQL] Failed to execute mass order insert", "error", err, "batch_size", len(orders))
		return fmt.Errorf("failed to insert order batch into postgres: %w", err)
	}

	// =========================================================================
	// 2. МАССОВОЕ ОБНОВЛЕНИЕ СТАТУСОВ МЕСТ В ТАБЛИЦЕ SEATS ЗА ОДИН ЗАПРОС
	// =========================================================================
	updateQuery := `
		UPDATE seats 
		SET status = 'RESERVED'
		FROM (VALUES %s) AS tmp(seat_id)
		WHERE seats.seat_id = tmp.seat_id;
	`

	valueStrings := make([]string, len(orders))
	valueArgs := make([]any, len(orders))
	for i, order := range orders {
		valueStrings[i] = fmt.Sprintf("($%d)", i+1)
		valueArgs[i] = order.TicketID
	}

	finalUpdateQuery := fmt.Sprintf(updateQuery, strings.Join(valueStrings, ", "))
	_, err = tx.Exec(ctx, finalUpdateQuery, valueArgs...)
	if err != nil {
		slog.Error("❌ [PostgreSQL] Failed to execute mass seats update", "error", err, "batch_size", len(orders))
		return fmt.Errorf("failed to update seats status to RESERVED in batch: %w", err)
	}

	// Фиксируем изменения, если оба массовых шага прошли успешно
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit postgres transaction: %w", err)
	}

	return nil
}

// CancelExpiredOrders находит просроченные брони, переводит их в статус CANCELLED
// и автоматически освобождает места в таблице seats, возвращая им статус AVAILABLE.
func (p *PostgresRepo) CancelExpiredOrders(ctx context.Context, expiryDuration time.Duration) ([]domain.ExpiredOrder, error) {
	cutoff := time.Now().Add(-expiryDuration)

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin cron transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// Используем CTE для атомарного апдейта двух таблиц за ОДИН проход СУБД
	query := `
		WITH updated_orders AS (
			UPDATE orders
			SET status = 'CANCELLED'
			WHERE status = 'RESERVED' AND created_at < $1
			RETURNING id, ticket_id
		)
		UPDATE seats
		SET status = 'AVAILABLE'
		FROM updated_orders
		WHERE seats.seat_id = updated_orders.ticket_id
		RETURNING updated_orders.id, updated_orders.ticket_id;
	`

	rows, err := tx.Query(ctx, query, cutoff)
	if err != nil {
		return nil, fmt.Errorf("failed to execute atomic cron update: %w", err)
	}
	defer rows.Close()

	var expiredOrders []domain.ExpiredOrder
	for rows.Next() {
		var eo domain.ExpiredOrder
		if err := rows.Scan(&eo.ID, &eo.TicketID); err != nil {
			return nil, fmt.Errorf("failed to scan expired order row: %w", err)
		}
		expiredOrders = append(expiredOrders, eo)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error in cron: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit cron transaction: %w", err)
	}

	return expiredOrders, nil
}

// GetAvailableSeats выгружает только свободные места для инициализации кэша.
// Используется строго при старте приложения для первичного прогрева кэша Redis.
func (p *PostgresRepo) GetAvailableSeats(ctx context.Context) ([]string, error) {
	query := "SELECT seat_id FROM seats WHERE status = 'AVAILABLE'"

	rows, err := p.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query available seats: %w", err)
	}
	defer rows.Close()

	var seats []string
	for rows.Next() {
		var seatID string
		if err := rows.Scan(&seatID); err != nil {
			return nil, fmt.Errorf("failed to scan seat_id: %w", err)
		}
		seats = append(seats, seatID)
	}

	return seats, rows.Err()
}
