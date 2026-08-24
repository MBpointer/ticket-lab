package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"ticket-lab/internal/domain"
)

// StartBatchConsumer запускает цикл накопления заказов из Kafka.
//
// Батч отправляется в PostgreSQL:
//   - при достижении batchSize;
//   - либо по истечении batchTimeout.
//
// ВАЖНО:
// если PostgreSQL не принял батч,
// новые Kafka-сообщения НЕ читаются поверх старого батча.
// Мы прекращаем цикл и даём приложению корректно завершиться.
// Kafka offset при этом не коммитится.
func (wm *WorkerManager) StartBatchConsumer(ctx context.Context) {
	var currentBatch []*domain.Order
	var lastOffset int64

	ticker := time.NewTicker(wm.batchTimeout)
	defer ticker.Stop()

	slog.Info("📥 Kafka Batch Consumer successfully started in background...")

	for {
		if len(currentBatch) >= wm.batchSize {
			if err := wm.flushBatch(ctx, &currentBatch, &lastOffset); err != nil {
				slog.Error("❌ CRITICAL: stopping Kafka consumer because PostgreSQL batch failed", "error", err, "batch_size", len(currentBatch))
				return
			}
			ticker.Reset(wm.batchTimeout)
			continue
		}

		select {
		case <-ctx.Done():
			if len(currentBatch) > 0 {
				_ = wm.flushBatch(ctx, &currentBatch, &lastOffset)
			}
			return

		case <-ticker.C:
			if len(currentBatch) > 0 {
				if err := wm.flushBatch(ctx, &currentBatch, &lastOffset); err != nil {
					slog.Error("❌ CRITICAL: stopping Kafka consumer because PostgreSQL batch failed", "error", err, "batch_size", len(currentBatch))
					return
				}
			}
			ticker.Reset(wm.batchTimeout)

		default:
			_, rawValue, offset, err := wm.kafkaConsumerRepo.FetchRawMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}

			var order domain.Order
			if err := json.Unmarshal(rawValue, &order); err != nil {
				slog.Error("❌ Failed to unmarshal kafka message, skipping", "error", err, "offset", offset)
				if err := wm.kafkaConsumerRepo.CommitOffset(ctx, offset); err != nil {
					return
				}
				continue
			}

			currentBatch = append(currentBatch, &order)
			lastOffset = offset
		}
	}
}
