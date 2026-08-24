package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"ticket-lab/internal/app/logger"
	"ticket-lab/internal/delivery/http"
	"ticket-lab/internal/domain"
	"ticket-lab/internal/repository/kafka/producer"
	"ticket-lab/internal/repository/postgres"
	"ticket-lab/internal/repository/redis"
	"ticket-lab/internal/service"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger.Init("ticket-api")
	slog.Info("Starting API Server infrastructure...")

	// 1. Инициализируем инфраструктурные пулы
	pgRepo := postgres.MustInit(ctx)
	redisRepo := redis.MustInit(ctx)

	// 2. Явно приводим к SOLID-интерфейсам для выполнения конкретной задачи
	var dbReader domain.SeatInventoryReader = pgRepo
	var cacheWarmuper domain.CacheWarmuper = redisRepo

	// 3. Выполняем синхронизацию
	slog.Info("Syncing: Postgres (Source of Truth) -> Redis (Cache)...")
	availableSeats, err := dbReader.GetAvailableSeats(ctx)
	if err != nil {
		slog.Error("❌ CRITICAL: failed to fetch seats from DB", "error", err)
		os.Exit(1)
	}

	if err := cacheWarmuper.WarmUpSeatInventory(ctx, "rock_concert_2026", availableSeats); err != nil {
		slog.Error("❌ CRITICAL: failed to warm up Redis", "error", err)
		os.Exit(1)
	}

	kafkaProducer := producer.MustInit(ctx)

	defer func() {
		slog.Info("Disconnecting infrastructure resources...")
		kafkaProducer.Close()
		redisRepo.Close()
		pgRepo.Close()
	}()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Сборка слоев (Dependency Injection)
	bookingService := service.NewBookingService(redisRepo, kafkaProducer)
	bookingHandler := http.NewBookingHandler(bookingService)

	// Запуск HTTP-сервера
	apiServer := http.NewHTTPServer(":"+port, bookingHandler, redisRepo)
	apiServer.Start(ctx)

	// Ожидаем системный сигнал завершения
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	slog.Info("Shutdown signal caught. Initiating graceful shutdown...")
	apiServer.WaitForShutdown()
}
