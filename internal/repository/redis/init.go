package redis

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisRepo struct {
	client *redis.Client
}

// MustInit создает RedisRepo и выполняет жесткий физический Ping
func MustInit(ctx context.Context) *RedisRepo {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	// Создаем сырого клиента
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// Ограничиваем таймаут на проверку связи с кэшем
	healthCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	// Выполняем физический Ping в Redis
	if err := client.Ping(healthCtx).Err(); err != nil {
		slog.Error("❌ CRITICAL REDIS ERROR: Application cannot start!\n"+
			"Failed to establish connection to Redis cache store.\n"+
			"Please check: 1) Is Redis Docker container running? 2) Is port 6379 free?", "error", err)
		_ = client.Close()
		os.Exit(1)
	}

	slog.Info("🔴 [Redis] Physical connection successfully verified (Ping OK).")

	return &RedisRepo{client: client}
}

// Close безопасно закрывает пул соединений с Redis
func (r *RedisRepo) Close() {
	if r.client != nil {
		slog.Info("🔌 [Redis] Closing connection client...")
		if err := r.client.Close(); err != nil {
			slog.Error("❌ [Redis] Error while closing client", "error", err)
		}
	}
}
