package service

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Назовем структуру по-другому, чтобы не путать с пакетами
type WorkerServer struct {
	manager       *WorkerManager
	metricsServer *http.Server
	workerDone    chan struct{}
}

func NewWorkerServer(manager *WorkerManager, metricsAddr string) *WorkerServer {
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())

	return &WorkerServer{
		manager: manager,
		metricsServer: &http.Server{
			Addr:    metricsAddr,
			Handler: metricsMux,
		},
		workerDone: make(chan struct{}),
	}
}

func (s *WorkerServer) Start(ctx context.Context) {
	// 1. Запуск сервера метрик Prometheus
	go func() {
		slog.Info("🚀 Worker Metrics HTTP server is starting...", "addr", s.metricsServer.Addr)
		if err := s.metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("❌ Error running Worker metrics server", "error", err)
		}
	}()

	// 2. Запуск фонового консьюмера Kafka
	go func() {
		s.manager.StartBatchConsumer(ctx)
		close(s.workerDone)
	}()

	// 3. Запуск крона очистки
	go s.manager.StartExpiredHoldCron(ctx, 1*time.Minute, 10*time.Minute)
}

func (s *WorkerServer) Stop() {
	shutdownTimeout := time.NewTimer(10 * time.Second)
	defer shutdownTimeout.Stop()

	// Мягко тушим сервер метрик
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.metricsServer.Shutdown(ctx)

	// Ожидаем завершения обработки батчей
	select {
	case <-s.workerDone:
		slog.Info("👋 All batch consumer tasks successfully finished. Safe to exit.")
	case <-shutdownTimeout.C:
		slog.Error("❌ Critical Alert: Graceful shutdown timed out! Some workers were forced killed.")
	}
}
