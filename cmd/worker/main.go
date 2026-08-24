package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ticket-lab/internal/app/logger"
	"ticket-lab/internal/repository/kafka/consumer"
	"ticket-lab/internal/repository/postgres"
	"ticket-lab/internal/repository/redis"
	"ticket-lab/internal/service"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger.Init("ticket-worker")

	slog.Info("Starting Worker Infrastructure...")

	// Инициализация инфраструктуры (адаптеров БД и очередей)
	pgRepo := postgres.MustInit(ctx)
	redisRepo := redis.MustInit(ctx)
	kafkaConsumer := consumer.MustInit(ctx)

	// Гарантированное закрытие соединений при выходе
	defer func() {
		slog.Info("Safely disconnecting worker infrastructure resources...")
		kafkaConsumer.Close()
		redisRepo.Close()
		pgRepo.Close()
	}()

	// Сборка бизнес-логики воркера
	workerManager := service.NewWorkerManager(
		pgRepo,
		kafkaConsumer,
		redisRepo,
		1000,
		1*time.Second,
	)

	// Запуск оркестратора воркеров и сервера метрик
	workerServer := service.NewWorkerServer(workerManager, ":8081")
	workerServer.Start(ctx)

	// Ожидание системного сигнала на закрытие приложения
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	slog.Info("Shutdown signal caught. Stopping background workers...")
	cancel() // Отправляем сигнал отмены контекста воркерам для остановки чтения

	workerServer.Stop() // Мягко дожидаемся обработки текущих батчей
}
