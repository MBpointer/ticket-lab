package service

import (
	"time"

	"ticket-lab/internal/domain"
)

// WorkerManager оркестрирует фоновые процессы: батчинг Kafka и Крон-таймер
type WorkerManager struct {
	pgRepo            domain.PostgresRepository      // Абстракция базы данных Postgres
	kafkaConsumerRepo domain.KafkaConsumerRepository // 🔥 Чистый интерфейс роли консьюмера без утечки сторонних библиотек
	redisRepo         domain.RedisRepository         // Абстракция нашего высоконагруженного кэша Redis
	batchSize         int                            // Лимит сообщений для отправки пачкой (например, 1000)
	batchTimeout      time.Duration                  // Максимальное время ожидания накопления пачки (например, 1 сек)
}

// NewWorkerManager — конструктор для инициализации менеджера воркеров по канонам Clean Architecture
func NewWorkerManager(
	pg domain.PostgresRepository,
	kafkaConsumer domain.KafkaConsumerRepository, // 🔥 Принимаем доменный интерфейс потребителя
	redis domain.RedisRepository,
	size int,
	timeout time.Duration,
) *WorkerManager {
	return &WorkerManager{
		pgRepo:            pg,
		kafkaConsumerRepo: kafkaConsumer,
		redisRepo:         redis,
		batchSize:         size,
		batchTimeout:      timeout,
	}
}
