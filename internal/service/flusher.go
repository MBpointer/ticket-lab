package service

import (
	"context"
	"log/slog"
	"time"

	"ticket-lab/internal/domain"
	"ticket-lab/internal/metrics"
)

// flushBatch:
//  1. удаляет дубли внутри текущего batch;
//  2. записывает уникальные заказы в PostgreSQL;
//  3. только после успешной записи коммитит Kafka offset;
//  4. обновляет статусы заказов в Redis пачкой через Pipeline;
//  5. очищает batch.
//
// Если PostgreSQL вернул ошибку — Kafka offset НЕ коммитится.
func (wm *WorkerManager) flushBatch(
	ctx context.Context,
	batch *[]*domain.Order,
	lastOffset *int64,
) error {
	if len(*batch) == 0 {
		return nil
	}

	// Дедупликация внутри одного batch.
	seenIDs := make(map[string]struct{}, len(*batch))
	uniqueBatch := make([]*domain.Order, 0, len(*batch))

	for _, order := range *batch {
		if _, exists := seenIDs[order.ID]; exists {
			slog.Warn("⚠️ Duplicate Order ID filtered from batch", "order_id", order.ID)
			continue
		}
		seenIDs[order.ID] = struct{}{}
		uniqueBatch = append(uniqueBatch, order)
	}

	if len(uniqueBatch) == 0 {
		*batch, *lastOffset = nil, 0
		return nil
	}

	slog.Info("📦 Batch ready for PostgreSQL", "original_size", len(*batch), "unique_size", len(uniqueBatch))
	start := time.Now()

	// Запись пачки заказов в СУБД за 2 массовых сетевых запроса
	err := wm.pgRepo.SaveOrdersBatch(ctx, uniqueBatch)
	metrics.PostgresBatchDuration.Observe(time.Since(start).Seconds())

	if err != nil {
		// КРИТИЧЕСКИ ВАЖНО: batch НЕ очищаем, Kafka offset НЕ коммитим.
		return err
	}

	// PostgreSQL успешно записал весь batch. Только теперь подтверждаем Kafka.
	if err := wm.kafkaConsumerRepo.CommitOffset(ctx, *lastOffset); err != nil {
		return err
	}

	// ОПТИМИЗАЦИЯ СЕТИ: Собираем ID транзакций и обновляем все статусы в Redis за 1 сетевой Pipeline-запрос
	orderIDs := make([]string, len(uniqueBatch))
	for i, order := range uniqueBatch {
		orderIDs[i] = order.ID
	}

	if err := wm.redisRepo.CacheTransactionStatusesBatch(ctx, orderIDs, "VERIFIED", 10*time.Minute); err != nil {
		slog.Warn("⚠️ Failed to update transaction statuses batch in Redis", "error", err)
	}

	// Только после полного успеха очищаем batch.
	*batch, *lastOffset = nil, 0
	return nil
}
