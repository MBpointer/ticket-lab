package domain

import (
	"context"
	"time"
)

// PostgresRepository описывает контракт для работы с БД заказов
type PostgresRepository interface {
	SaveOrdersBatch(ctx context.Context, orders []*Order) error
	CancelExpiredOrders(ctx context.Context, expiryDuration time.Duration) ([]ExpiredOrder, error)
}

// SeatInventoryReader предоставляет узкий контракт только для чтения инвентаря мест (SOLID)
type SeatInventoryReader interface {
	GetAvailableSeats(ctx context.Context) ([]string, error)
}

// RedisRepository описывает контракт для оперативного кэша, блокировок и бронирования мест
type RedisRepository interface {
	// 🔥 Бронирование и освобождение мест в оперативной памяти (Lua-скрипты)
	ReserveSeat(ctx context.Context, eventID, seatID, orderID string) (bool, error)
	ReleaseSeat(ctx context.Context, eventID, seatID, orderID string) (bool, error)

	// Распределенные блокировки для защиты от Race Condition
	AcquireTransactionLock(ctx context.Context, lockKey string, duration time.Duration) (bool, error)

	// Кэширование статусов транзакций для быстрой проверки с API
	CacheTransactionStatus(ctx context.Context, orderID string, status string, ttl time.Duration) error
	GetTransactionStatus(ctx context.Context, orderID string) (string, error)

	CacheTransactionStatusesBatch(ctx context.Context, orderIDs []string, status string, ttl time.Duration) error
}

// CacheWarmuper предоставляет контракт только для прогрева кэша
type CacheWarmuper interface {
	WarmUpSeatInventory(ctx context.Context, eventID string, seatIDs []string) error
}

// KafkaProducerRepository описывает контракт отправки сообщений (использует API)
type KafkaProducerRepository interface {
	PublishOrder(ctx context.Context, order *Order) error
}

// KafkaConsumerRepository описывает контракт чтения сообщений (использует Воркер)
type KafkaConsumerRepository interface {
	FetchRawMessage(ctx context.Context) (msgID string, value []byte, offset int64, err error)
	CommitOffset(ctx context.Context, offset int64) error
}
