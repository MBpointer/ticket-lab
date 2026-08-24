package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"ticket-lab/internal/metrics"
)

const (
	seatsHashTTL       = 24 * time.Hour
	availableSeatValue = "AVAILABLE"
)

// WarmUpSeatInventory атомарно прогревает кэш переданными ID мест из СУБД.
// Создает всю карту мест пачкой за 1 сетевой запрос через Pipeline.
func (r *RedisRepo) WarmUpSeatInventory(ctx context.Context, eventID string, seatIDs []string) error {
	start := time.Now()

	defer func() {
		metrics.RedisOperationDuration.
			WithLabelValues("warmup_seat_inventory").
			Observe(time.Since(start).Seconds())
	}()

	key := "event:seats:" + eventID

	// Проверяем, существует ли карта. При перезапуске приложения нельзя превратить уже забронированные места обратно в AVAILABLE.
	exists, err := r.client.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to check seat cache: %w", err)
	}

	// Если кэш уже есть, не перезаписываем его (там могут быть активные PENDING брони)
	if exists > 0 {
		slog.Info("🍏 Redis seat inventory already exists, skipping full warmup", "event_id", eventID)
		r.client.Expire(ctx, key, seatsHashTTL)
		return nil
	}

	// Карт в кэше нет — создаем её пачкой на основе актуального состояния СУБД
	pipe := r.client.Pipeline()
	for _, seatID := range seatIDs {
		pipe.HSet(ctx, key, seatID, availableSeatValue)
	}
	pipe.Expire(ctx, key, seatsHashTTL)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute pipeline warmup: %w", err)
	}

	slog.Info("✅ Redis seat inventory warmed up successfully from DB state", "event_id", eventID, "seats_loaded", len(seatIDs))
	return nil
}
