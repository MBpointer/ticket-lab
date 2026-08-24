package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"ticket-lab/internal/metrics"
)

// ReserveSeat атомарно резервирует существующее свободное место.
//
// Возможны три состояния:
//
//	AVAILABLE -> orderID  => success
//	orderID   -> orderID  => failure
//	отсутствует поле       => failure
//
// Таким образом ticket_10000 не может внезапно появиться.
func (r *RedisRepo) ReserveSeat(
	ctx context.Context,
	eventID,
	seatID,
	orderID string,
) (bool, error) {
	start := time.Now()

	defer func() {
		metrics.RedisOperationDuration.
			WithLabelValues("reserve_seat").
			Observe(time.Since(start).Seconds())
	}()

	key := "event:seats:" + eventID

	result, err := reserveSeat.Run(
		ctx,
		r.client,
		[]string{key},
		seatID,
		orderID,
		availableSeatValue,
	).Int()

	if err != nil {
		slog.Error(
			"❌ [Redis] Failed to reserve seat via Lua script",
			"error",
			err,
			"event_id",
			eventID,
			"seat_id",
			seatID,
		)

		return false, fmt.Errorf("redis reserve seat failed: %w", err)
	}

	return result == 1, nil
}

// ReleaseSeat освобождает место только у текущего владельца.
//
// orderID -> AVAILABLE
//
// Поле остается в Hash, поэтому количество мест всегда равно изначально загруженному.
func (r *RedisRepo) ReleaseSeat(
	ctx context.Context,
	eventID,
	seatID,
	orderID string,
) (bool, error) {
	start := time.Now()

	defer func() {
		metrics.RedisOperationDuration.
			WithLabelValues("release_seat").
			Observe(time.Since(start).Seconds())
	}()

	key := "event:seats:" + eventID

	result, err := releaseSeatScriptObj.Run(
		ctx,
		r.client,
		[]string{key},
		seatID,
		orderID,
		availableSeatValue,
	).Int()

	if err != nil {
		slog.Error(
			"❌ [Redis] Failed to release seat via Lua script",
			"error",
			err,
			"event_id",
			eventID,
			"seat_id",
			seatID,
		)

		return false, fmt.Errorf("redis release seat failed: %w", err)
	}

	return result == 1, nil
}
