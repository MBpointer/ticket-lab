package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"ticket-lab/internal/domain"
	"ticket-lab/internal/metrics"
)

// StartExpiredHoldCron запускает фоновую периодическую очистку истёкших броней.
func (wm *WorkerManager) StartExpiredHoldCron(ctx context.Context, checkInterval, expiryDuration time.Duration) {
	cronTicker := time.NewTicker(checkInterval)
	defer cronTicker.Stop()

	slog.Info("⏳ Expired Hold Cron-Worker successfully started in background...")
	const eventID = "rock_concert_2026"

	for {
		select {
		case <-ctx.Done():
			return
		case <-cronTicker.C:
			releasedOrders, err := wm.pgRepo.CancelExpiredOrders(ctx, expiryDuration)
			if err != nil {
				slog.Error("❌ Cron critical error: failed to cancel expired orders in DB", "error", err)
				continue
			}

			if len(releasedOrders) == 0 {
				continue
			}

			slog.Info("⏰ Cron: found expired orders. Returning tickets to circulation...", "released_count", len(releasedOrders))
			metrics.CancelledTicketsCounter.Add(float64(len(releasedOrders)))

			// Вызываем конкурентное освобождение мест в Redis
			failedCount := wm.releaseExpiredSeats(ctx, eventID, releasedOrders)

			slog.Info("⏰ Cron: finished updating Redis seats map for expired holds",
				"total", len(releasedOrders),
				"failed", failedCount,
			)
		}
	}
}

// releaseExpiredSeats параллельно освобождает места в Redis с ограничением concurrency до 32 воркеров.
// Полностью сохраняет оригинальную телеметрию ошибок для отображения на дашборде Grafana.
func (wm *WorkerManager) releaseExpiredSeats(ctx context.Context, eventID string, orders []domain.ExpiredOrder) int {
	jobs := make(chan domain.ExpiredOrder, len(orders))
	for _, order := range orders {
		jobs <- order
	}
	close(jobs)

	var wg sync.WaitGroup
	var mu sync.Mutex
	failedCount := 0

	workerCount := 32
	if workerCount > len(orders) {
		workerCount = len(orders)
	}

	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for order := range jobs {
				released, err := wm.redisRepo.ReleaseSeat(ctx, eventID, order.TicketID, order.ID)
				if err != nil {
					slog.Error("❌ Cron error: failed to release seat in Redis map",
						"event_id", eventID, "ticket_id", order.TicketID, "order_id", order.ID, "error", err)

					mu.Lock()
					failedCount++
					mu.Unlock()
					metrics.CronReleaseErrorsCounter.Inc() // Метрика для вашей панели ошибок в Grafana
					continue
				}
				if !released {
					slog.Warn("⚠️ Expired seat was not released — ownership may have changed",
						"event_id", eventID, "ticket_id", order.TicketID, "order_id", order.ID)
					continue
				}
				metrics.CronReleasedTicketsCounter.Inc()
			}
		}()
	}
	wg.Wait()
	return failedCount
}
