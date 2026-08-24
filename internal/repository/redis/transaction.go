package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"ticket-lab/internal/metrics"

	"github.com/redis/go-redis/v9"
)

const (
	maxRetries     = 3
	retryBaseDelay = 50 * time.Millisecond
	retryMaxDelay  = 500 * time.Millisecond
)

// withRetry выполняет операцию с повторными попытками при временных ошибках.
func withRetry(ctx context.Context, op func(ctx context.Context) error) error {
	var lastErr error
	delay := retryBaseDelay

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			delay = min(delay*2, retryMaxDelay)
		}

		lastErr = op(ctx)
		if lastErr == nil {
			return nil
		}

		// Не повторяем при контексте или постоянных ошибках
		if !isRetryableError(lastErr) {
			return lastErr
		}

		slog.Debug("Redis operation failed, retrying", "attempt", attempt+1, "error", lastErr)
	}
	return fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}

// isRetryableError определяет, стоит ли повторять операцию.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	// Сетевые ошибки, timeout, connection refused — повторяем
	return err != redis.Nil
}

func (r *RedisRepo) AcquireTransactionLock(ctx context.Context, lockKey string, lockDuration time.Duration) (bool, error) {
	start := time.Now()
	defer func() {
		metrics.RedisOperationDuration.WithLabelValues("acquire_lock").Observe(time.Since(start).Seconds())
	}()

	key := "lock:tx:" + lockKey
	var acquired bool
	err := withRetry(ctx, func(ctx context.Context) error {
		result, e := r.client.SetNX(ctx, key, "locked", lockDuration).Result()
		if e != nil {
			return e
		}
		acquired = result
		return nil
	})
	if err != nil {
		slog.Error("❌ [Redis] Failed to acquire transaction lock", "error", err, "lock_key", lockKey)
		return false, err
	}
	return acquired, nil
}

func (r *RedisRepo) CacheTransactionStatus(ctx context.Context, orderID string, status string, ttl time.Duration) error {
	start := time.Now()
	defer func() {
		metrics.RedisOperationDuration.WithLabelValues("cache_tx_status").Observe(time.Since(start).Seconds())
	}()

	key := "tx:status:" + orderID
	return withRetry(ctx, func(ctx context.Context) error {
		return r.client.Set(ctx, key, status, ttl).Err()
	})
}

func (r *RedisRepo) GetTransactionStatus(ctx context.Context, orderID string) (string, error) {
	start := time.Now()
	defer func() {
		metrics.RedisOperationDuration.WithLabelValues("get_tx_status").Observe(time.Since(start).Seconds())
	}()

	key := "tx:status:" + orderID
	var status string
	err := withRetry(ctx, func(ctx context.Context) error {
		s, err := r.client.Get(ctx, key).Result()
		if err == redis.Nil {
			// Возвращаем redis.Nil явно, чтобы вызывающий код мог отличить "не найдено" от "пустой статус"
			return redis.Nil
		}
		if err != nil {
			return err
		}
		status = s
		return nil
	})
	if err != nil {
		if err == redis.Nil {
			// Ключ не найден — это не ошибка, возвращаем (nil, redis.Nil)
			return "", redis.Nil
		}
		slog.Error("❌ [Redis] Error reading transaction status", "error", err, "order_id", orderID)
		return "", err
	}
	return status, nil
}

// CacheTransactionStatusesBatch обновляет статусы целой пачки транзакций за ОДИН сетевой запрос через Pipeline.
func (r *RedisRepo) CacheTransactionStatusesBatch(ctx context.Context, orderIDs []string, status string, ttl time.Duration) error {
	start := time.Now()
	defer func() {
		metrics.RedisOperationDuration.WithLabelValues("cache_tx_status_batch").Observe(time.Since(start).Seconds())
	}()

	pipe := r.client.Pipeline()
	for _, orderID := range orderIDs {
		key := "tx:status:" + orderID
		pipe.Set(ctx, key, status, ttl)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		slog.Error("❌ [Redis] Failed to execute batch status update via pipeline", "error", err, "count", len(orderIDs))
		return fmt.Errorf("redis batch pipeline failed: %w", err)
	}
	return nil
}
